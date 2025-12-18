// src/core/cache.ts
import { Env } from './config';

export class ContentCache {
  private cache: Cache;
  private env: Env;

  constructor(env: Env) {
    this.cache = (caches as any).default;
    this.env = env;
  }

  // 尝试获取缓存的响应
  async match(request: Request): Promise<Response | undefined> {
    const ttl = parseInt(this.env.CACHE_TTL_CONTENT) || 0;
    if (ttl <= 0) return undefined;

    return await this.cache.match(request);
  }

  // 写入缓存
  async put(request: Request, response: Response) {
    const ttl = parseInt(this.env.CACHE_TTL_CONTENT) || 0;
    if (ttl <= 0) return;

    // 必须重新构造 Response 才能修改 Headers (Response 对象是不可变的)
    // 且 Cache API 要求 response 必须包含 Cache-Control max-age
    const headers = new Headers(response.headers);
    headers.set('Cache-Control', `public, max-age=${ttl}`);
    
    // 如果之前响应没设置 Content-Length 等关键头，这里最好保留原样
    // 注意：Cloudflare Cache API 不支持缓存 Partial Content (206)，
    // 但如果源站返回 200，我们可以缓存它。
    
    const cachedResponse = new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: headers
    });

    // 这是一个副作用操作，需要在 ctx.waitUntil 中调用，或者由调用者 await
    // 这里的 body stream 已经被读取用于构造新响应，需要注意克隆问题
    // 在 index.ts 中调用时，我们将传入 response.clone()
    await this.cache.put(request, cachedResponse);
  }
}