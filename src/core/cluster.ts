// src/core/cluster.ts
import { AwsClient } from 'aws4fetch';
import { XMLParser } from 'fast-xml-parser';
import { Env, BucketConfig, parseBucketsConfig } from './config';
import { UnifiedCache } from './cache';

export class ClusterManager {
  private clients: Map<string, AwsClient>; // bucketName -> AwsClient
  private configs: BucketConfig[]; // 保存配置以便获取 endpoint
  private env: Env;
  private cache: UnifiedCache;
  private xmlParser: XMLParser;

  constructor(env: Env, cache: UnifiedCache) {
    this.env = env;
    this.cache = cache;
    this.configs = parseBucketsConfig(env.BUCKETS_CONFIG);
    this.clients = new Map();
    this.xmlParser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: "@_"
    });

    // 初始化 aws4fetch 客户端
    this.configs.forEach(cfg => {
      // 从 https://s3.region.backblazeb2.com 提取 region 和 service
      // aws4fetch 会自动处理 host
      const client = new AwsClient({
        accessKeyId: cfg.accessKeyId,
        secretAccessKey: cfg.secretAccessKey,
        service: 's3',
        region: cfg.region,
      });
      this.clients.set(cfg.name, client);
    });
  }

  // 获取配置信息（Endpoint等）
  private getConfig(bucketName: string): BucketConfig | undefined {
    return this.configs.find(c => c.name === bucketName);
  }

  // 辅助：构建完整 URL
  private buildUrl(bucketName: string, key: string, query?: URLSearchParams): string {
    const cfg = this.getConfig(bucketName);
    if (!cfg) throw new Error(`Configuration for bucket ${bucketName} not found`);
    
    // 确保 endpoint 不带末尾斜杠，key 不带开头斜杠
    const baseUrl = cfg.endpoint.replace(/\/$/, '');
    const cleanKey = key.replace(/^\//, '');
    let url = `${baseUrl}/${bucketName}/${cleanKey}`;
    if (query) {
      url += `?${query.toString()}`;
    }
    return url;
  }

  // 定位文件 (HEAD)
  async locateFile(key: string): Promise<{ bucket: string, size: number, type: string, lastModified: Date } | null> {
    // 1. 查缓存
    const cachedBucket = await this.cache.getFileBucket(key);
    if (cachedBucket && this.clients.has(cachedBucket)) {
       // 即使命中缓存，为了获取最新元数据，建议还是 HEAD 一次，或者在此处信任缓存直接返回
       // 这里为了演示完整性，如果命中缓存直接尝试 HEAD 确认
       const res = await this.headObject(cachedBucket, key);
       if (res) return res;
    }

    // 2. 并发查询所有桶
    const promises = this.configs.map(cfg => this.headObject(cfg.name, key));
    const results = await Promise.all(promises);
    
    // 过滤有效结果并取最新
    const validResults = results.filter(r => r !== null).sort((a, b) => {
        return (b!.lastModified.getTime() || 0) - (a!.lastModified.getTime() || 0);
    });

    if (validResults.length > 0) {
      const best = validResults[0]!;
      await this.cache.setFileBucket(key, best.bucket);
      return best;
    }

    return null;
  }

  // 内部 HEAD 实现
  private async headObject(bucketName: string, key: string) {
    const client = this.clients.get(bucketName);
    if (!client) return null;

    try {
      const url = this.buildUrl(bucketName, key);
      const res = await client.fetch(url, { method: 'HEAD' });
      
      if (res.status === 200) {
        return {
          bucket: bucketName,
          size: parseInt(res.headers.get('Content-Length') || '0'),
          type: res.headers.get('Content-Type') || 'application/octet-stream',
          lastModified: new Date(res.headers.get('Last-Modified') || new Date())
        };
      }
    } catch (e) {
      // ignore error
    }
    return null;
  }

  // 执行下载 (GET) - 返回 Response
  async getObject(bucketName: string, key: string, range?: string): Promise<Response> {
    const client = this.clients.get(bucketName);
    if (!client) throw new Error('Bucket client not found');

    const url = this.buildUrl(bucketName, key);
    const headers: Record<string, string> = {};
    if (range) headers['Range'] = range;

    const res = await client.fetch(url, { method: 'GET', headers });
    
    // 重新构建 Response 以确保 header干净，或者直接透传
    // aws4fetch 返回的是标准 Response
    return new Response(res.body, {
      status: res.status,
      statusText: res.statusText,
      headers: res.headers
    });
  }

  // 执行上传 (PUT)
  async putObject(bucketName: string, key: string, body: ReadableStream | null, headers: Headers) {
    const client = this.clients.get(bucketName);
    if (!client) throw new Error('Bucket client not found');

    const url = this.buildUrl(bucketName, key);
    
    // 提取关键头信息
    const putHeaders: Record<string, string> = {};
    if (headers.get('Content-Type')) putHeaders['Content-Type'] = headers.get('Content-Type')!;
    if (headers.get('Content-Length')) putHeaders['Content-Length'] = headers.get('Content-Length')!;
    
    // aws4fetch 会自动处理签名
    const res = await client.fetch(url, {
      method: 'PUT',
      body: body,
      headers: putHeaders
    });

    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`Upload failed: ${res.status} ${res.statusText} - ${errText}`);
    }
    
    return res;
  }

  // 执行删除 (DELETE)
  async deleteObject(bucketName: string, key: string) {
    const client = this.clients.get(bucketName);
    if (!client) return;
    const url = this.buildUrl(bucketName, key);
    await client.fetch(url, { method: 'DELETE' });
  }

  // 选桶策略
  async selectBucketForUpload(fileSize: number): Promise<string> {
    const maxBytes = (parseInt(this.env.MAX_BUCKET_SIZE_GB) || 10) * 1024 * 1024 * 1024;
    const strategy = this.env.UPLOAD_STRATEGY || 'fill-first';

    const usages: { name: string, usage: number }[] = [];
    for (const cfg of this.configs) {
      const val = await this.env.BUCKET_STATE_KV.get(`usage:${cfg.name}`);
      usages.push({ name: cfg.name, usage: val ? parseInt(val) : 0 });
    }

    let selected: string | null = null;

    if (strategy === 'balanced') {
      usages.sort((a, b) => a.usage - b.usage);
      const candidate = usages.find(u => u.usage + fileSize < maxBytes);
      selected = candidate ? candidate.name : null;
    } else {
      const candidate = usages.find(u => u.usage + fileSize < maxBytes);
      selected = candidate ? candidate.name : null;
    }

    if (!selected) {
        // 如果都满了或者没有数据，降级回第一个配置的桶
        console.warn('All buckets full or KV error, defaulting to first bucket');
        return this.configs[0].name;
    }
    return selected;
  }

  async incrementUsage(bucketName: string, bytes: number) {
    const key = `usage:${bucketName}`;
    const current = await this.env.BUCKET_STATE_KV.get(key);
    const newVal = (current ? parseInt(current) : 0) + bytes;
    await this.env.BUCKET_STATE_KV.put(key, newVal.toString());
  }

  // 聚合 List
  async aggregateList(prefix: string = ''): Promise<any[]> {
    const promises = this.configs.map(async (cfg) => {
      const client = this.clients.get(cfg.name)!;
      try {
        const query = new URLSearchParams({ prefix });
        // S3 ListObjectsV2
        query.set('list-type', '2'); 
        const url = this.buildUrl(cfg.name, '', query);
        
        const res = await client.fetch(url, { method: 'GET' });
        if (!res.ok) throw new Error(res.statusText);
        
        const xmlText = await res.text();
        const parsed = this.xmlParser.parse(xmlText);
        
        // 处理 fast-xml-parser 的输出结构
        // ListBucketResult.Contents 可能是数组或单个对象
        const result = parsed.ListBucketResult;
        if (!result || !result.Contents) return [];
        
        const contents = Array.isArray(result.Contents) ? result.Contents : [result.Contents];
        
        return contents.map((item: any) => ({
          Key: item.Key,
          LastModified: new Date(item.LastModified),
          ETag: item.ETag,
          Size: parseInt(item.Size),
          _bucket: cfg.name
        }));

      } catch (e) {
        console.error(`List failed for ${cfg.name}`, e);
        return [];
      }
    });

    const results = await Promise.all(promises);
    const allFiles = results.flat();

    const fileMap = new Map<string, any>();
    allFiles.forEach(file => {
      const key = file.Key!;
      if (!fileMap.has(key)) {
        fileMap.set(key, file);
      } else {
        const existing = fileMap.get(key);
        if (file.LastModified > existing.LastModified) {
          fileMap.set(key, file);
        }
      }
    });

    return Array.from(fileMap.values());
  }
}