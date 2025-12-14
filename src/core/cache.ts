// src/core/cache.ts
import { Env } from './config';

export class UnifiedCache {
  private cache: Cache;
  private env: Env;

  constructor(env: Env) {
    this.cache = (caches as any).default;
    this.env = env;
  }

  // 获取文件所在的桶名
  async getFileBucket(path: string): Promise<string | null> {
    const cacheKey = new Request(`https://cache.internal/meta/${encodeURIComponent(path)}`);
    const response = await this.cache.match(cacheKey);
    if (response) {
      return await response.text();
    }
    return null;
  }

  // 设置文件所在的桶名
  async setFileBucket(path: string, bucketName: string) {
    const cacheKey = new Request(`https://cache.internal/meta/${encodeURIComponent(path)}`);
    const ttl = parseInt(this.env.CACHE_TTL_METADATA) || 3600;
    
    const response = new Response(bucketName, {
      headers: { 'Cache-Control': `public, max-age=${ttl}` }
    });
    // 使用 waitUntil 防止阻塞
    // 注意: 在 Worker 实例中调用时需要 ctx.waitUntil，这里简化处理，实际调用处需 await 或传入 ctx
    await this.cache.put(cacheKey, response);
  }

  // 删除文件位置缓存 (用于删除文件时)
  async deleteFileBucket(path: string) {
    const cacheKey = new Request(`https://cache.internal/meta/${encodeURIComponent(path)}`);
    await this.cache.delete(cacheKey);
  }

  // 缓存 List XML 结果
  async getListCache(cacheKeyStr: string): Promise<Response | undefined> {
    const cacheKey = new Request(cacheKeyStr);
    return await this.cache.match(cacheKey);
  }

  async setListCache(cacheKeyStr: string, body: string, contentType: string = 'application/xml') {
    const cacheKey = new Request(cacheKeyStr);
    const ttl = parseInt(this.env.CACHE_TTL_LIST) || 600;
    const response = new Response(body, {
      headers: { 
        'Content-Type': contentType,
        'Cache-Control': `public, max-age=${ttl}` 
      }
    });
    await this.cache.put(cacheKey, response);
  }
}