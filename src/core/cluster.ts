// src/core/cluster.ts
import { AwsClient } from 'aws4fetch';
import { XMLParser } from 'fast-xml-parser';
import { Env, BucketConfig, parseBucketsConfig } from './config';

// 定义索引项结构
interface FileMetadata {
  bucket: string;
  size: number;
  lastModified: number; // timestamp
  etag: string;
}

// 整个集群的文件索引 Map: filepath -> metadata
type ClusterIndex = Record<string, FileMetadata>;

export class ClusterManager {
  private clients: Map<string, AwsClient>;
  private configs: BucketConfig[];
  private env: Env;
  private xmlParser: XMLParser;
  
  // 内存缓存一份 index，避免单次请求内多次读取 KV
  private inMemoryIndex: ClusterIndex | null = null;
  private readonly KV_KEY = 'CLUSTER_INDEX';
  private readonly KV_TTL = 86400; // 1天

  constructor(env: Env) {
    this.env = env;
    this.configs = parseBucketsConfig(env.BUCKETS_CONFIG);
    this.clients = new Map();
    this.xmlParser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: "@_"
    });

    this.configs.forEach(cfg => {
      const client = new AwsClient({
        accessKeyId: cfg.accessKeyId,
        secretAccessKey: cfg.secretAccessKey,
        service: 's3',
        region: cfg.region,
      });
      this.clients.set(cfg.name, client);
    });
  }

  // === 索引核心逻辑 ===

  // 加载索引 (内存 -> KV -> 重建)
  private async loadIndex(forceRebuild = false): Promise<ClusterIndex> {
    if (this.inMemoryIndex && !forceRebuild) return this.inMemoryIndex;

    // 尝试从 KV 读取
    if (!forceRebuild) {
      const stored = await this.env.BUCKET_STATE_KV.get(this.KV_KEY, 'json');
      if (stored) {
        this.inMemoryIndex = stored as ClusterIndex;
        return this.inMemoryIndex;
      }
    }

    // 如果 KV 没有或强制重建
    console.log('Index missing or rebuild requested, rebuilding...');
    return await this.rebuildIndex();
  }

  // 保存索引到 KV
  private async saveIndex(index: ClusterIndex) {
    this.inMemoryIndex = index;
    await this.env.BUCKET_STATE_KV.put(this.KV_KEY, JSON.stringify(index), {
      expirationTtl: this.KV_TTL
    });
  }

  // 重建索引：遍历所有桶的所有文件
  async rebuildIndex(): Promise<ClusterIndex> {
    const newIndex: ClusterIndex = {};
    
    // 并发处理每个桶
    const promises = this.configs.map(async (cfg) => {
      const client = this.clients.get(cfg.name)!;
      let continuationToken: string | undefined = undefined;
      
      do {
        try {
          const query = new URLSearchParams({ 'list-type': '2' }); // Use ListObjectsV2
          if (continuationToken) {
            query.set('continuation-token', continuationToken);
          }

          const url = `${cfg.endpoint.replace(/\/$/, '')}/${cfg.name}/?${query.toString()}`;
          const res = await client.fetch(url, { method: 'GET' });
          if (!res.ok) {
            console.error(`Failed to list bucket ${cfg.name}: ${res.status}`);
            break;
          }

          const xmlText = await res.text();
          const parsed = this.xmlParser.parse(xmlText);
          const result = parsed.ListBucketResult;

          if (result && result.Contents) {
            const contents = Array.isArray(result.Contents) ? result.Contents : [result.Contents];
            for (const item of contents) {
              // 存入索引
              newIndex[item.Key] = {
                bucket: cfg.name,
                size: parseInt(item.Size),
                lastModified: new Date(item.LastModified).getTime(),
                etag: item.ETag
              };
            }
          }

          // 检查是否有下一页
          if (result && result.IsTruncated === 'true') {
            continuationToken = result.NextContinuationToken;
          } else {
            continuationToken = undefined;
          }

        } catch (e) {
          console.error(`Error listing bucket ${cfg.name}:`, e);
          break;
        }
      } while (continuationToken);
    });

    await Promise.all(promises);
    
    // 保存并返回
    await this.saveIndex(newIndex);
    return newIndex;
  }

  // 清除索引 (用于 API 手动清除)
  async clearIndex() {
    await this.env.BUCKET_STATE_KV.delete(this.KV_KEY);
    this.inMemoryIndex = null;
  }

  // === 业务逻辑 (基于索引) ===

  // 定位文件
  async locateFile(key: string): Promise<FileMetadata | null> {
    const index = await this.loadIndex();
    const meta = index[key];
    return meta || null;
  }

  // 列出文件 (支持 prefix 和 delimiter)
  async aggregateList(prefix: string = '', delimiter?: string): Promise<{ contents: any[], commonPrefixes: string[] }> {
    const index = await this.loadIndex();
    
    const contents: any[] = [];
    const commonPrefixes = new Set<string>();

    for (const [key, meta] of Object.entries(index)) {
      // 1. 检查前缀
      if (!key.startsWith(prefix)) continue;

      // 2. 处理分级逻辑
      const relativePath = key.substring(prefix.length);
      
      if (delimiter && relativePath.includes(delimiter)) {
        // 如果包含分隔符，提取 CommonPrefix (例如 "folder/")
        const dIndex = relativePath.indexOf(delimiter);
        const subFolder = prefix + relativePath.substring(0, dIndex + delimiter.length);
        commonPrefixes.add(subFolder);
      } else {
        // 直接文件或未指定分隔符
        contents.push({
          Key: key,
          LastModified: new Date(meta.lastModified),
          ETag: meta.etag,
          Size: meta.size,
          StorageClass: 'STANDARD'
        });
      }
    }

    // 排序内容 (按最后修改时间降序)
    contents.sort((a, b) => b.LastModified.getTime() - a.LastModified.getTime());
    
    // 返回内容和去重后的前缀（排序后）
    return {
      contents,
      commonPrefixes: Array.from(commonPrefixes).sort()
    };
  }

  // 选桶策略 (基于索引实时计算使用量)
  async selectBucketForUpload(fileSize: number): Promise<string | null> {
    const index = await this.loadIndex();
    const maxBytes = (parseFloat(this.env.MAX_BUCKET_SIZE_GB) || 10) * 1024 * 1024 * 1024;
    
    const bucketUsage = new Map<string, number>();
    this.configs.forEach(c => bucketUsage.set(c.name, 0));

    for (const meta of Object.values(index)) {
      const current = bucketUsage.get(meta.bucket) || 0;
      bucketUsage.set(meta.bucket, current + meta.size);
    }

    const strategy = this.env.UPLOAD_STRATEGY || 'fill-first';
    const usageList = Array.from(bucketUsage.entries()).map(([name, usage]) => ({ name, usage }));

    if (strategy === 'balanced') {
      usageList.sort((a, b) => a.usage - b.usage);
    }

    const candidate = usageList.find(u => u.usage + fileSize < maxBytes);

    if (!candidate) {
      console.warn('All buckets full for the given file size');
      return null;
    }
    
    return candidate.name;
  }

  // === 操作逻辑 (需要同步更新索引) ===

  private buildUrl(bucketName: string, key: string): string {
    const cfg = this.configs.find(c => c.name === bucketName);
    if (!cfg) throw new Error(`Bucket ${bucketName} not found`);
    return `${cfg.endpoint.replace(/\/$/, '')}/${bucketName}/${key.replace(/^\//, '')}`;
  }

  // 下载
  async getObject(bucketName: string, key: string, range?: string): Promise<Response> {
    const client = this.clients.get(bucketName);
    if (!client) throw new Error('Bucket client not found');

    const url = this.buildUrl(bucketName, key);
    const headers: Record<string, string> = {};
    if (range) headers['Range'] = range;

    const res = await client.fetch(url, { method: 'GET', headers });
    
    return new Response(res.body, {
      status: res.status,
      statusText: res.statusText,
      headers: res.headers
    });
  }

  // 上传
  async putObject(bucketName: string, key: string, body: ReadableStream | null, headers: Headers) {
    const client = this.clients.get(bucketName);
    if (!client) throw new Error('Bucket client not found');

    const url = this.buildUrl(bucketName, key);
    const putHeaders: Record<string, string> = {};
    if (headers.get('Content-Type')) putHeaders['Content-Type'] = headers.get('Content-Type')!;
    const length = headers.get('Content-Length');
    if (length) putHeaders['Content-Length'] = length;

    const res = await client.fetch(url, {
      method: 'PUT',
      body: body,
      headers: putHeaders
    });

    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`Upload failed: ${res.status} ${res.statusText} - ${errText}`);
    }

    const etag = res.headers.get('ETag') || 'unknown';
    const size = length ? parseInt(length) : 0; 
    
    const index = await this.loadIndex();
    index[key] = {
      bucket: bucketName,
      size: size,
      lastModified: Date.now(),
      etag: etag.replace(/"/g, '') // 去除引号
    };
    await this.saveIndex(index);
    
    return res;
  }

  // 删除
  async deleteObject(bucketName: string, key: string) {
    const client = this.clients.get(bucketName);
    if (!client) return;
    
    const url = this.buildUrl(bucketName, key);
    await client.fetch(url, { method: 'DELETE' });

    const index = await this.loadIndex();
    if (index[key]) {
      delete index[key];
      await this.saveIndex(index);
    }
  }
}