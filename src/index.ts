// src/index.ts
import { ClusterManager } from './core/cluster';
import { ContentCache } from './core/cache';
import { AuthMiddleware } from './core/auth';
import { Env } from './core/config';

// export { Env };

// 辅助：生成 S3 XML List 响应
function generateXmlList(files: any[], bucketName: string): string {
  const contents = files.map(f => `
    <Contents>
      <Key>${f.Key}</Key>
      <LastModified>${f.LastModified.toISOString()}</LastModified>
      <ETag>"${f.ETag}"</ETag>
      <Size>${f.Size}</Size>
      <StorageClass>STANDARD</StorageClass>
    </Contents>`).join('');
    
  return `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>${bucketName}</Name>
  <Prefix></Prefix>
  <KeyCount>${files.length}</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  ${contents}
</ListBucketResult>`;
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    // 处理 path: 如果是 /_cache 这种特殊路径需要特殊处理
    // 普通文件 key 去掉开头的 /    
    const key = url.pathname.substring(1); 
    
    // 1. 鉴权
    const auth = new AuthMiddleware(env);
    const isAuth = await auth.verify(request);
    
    if (!isAuth && !url.searchParams.has('token')) {
       return new Response('Unauthorized', { status: 403 });
    }

    const cache = new ContentCache(env);
    const cluster = new ClusterManager(env);

    try {
      // === 管理 API: 清除 KV 索引 ===
      if (request.method === 'DELETE' && url.pathname === '/_cache') {
        await cluster.clearIndex();
        return new Response('Cache Cleared', { status: 200 });
      }

      // === LIST OBJECTS (GET / or GET /?list-type=2) ===
      if (request.method === 'GET' && (key === '' || url.searchParams.has('list-type') || url.searchParams.has('prefix'))) {
        // 直接从 KV 索引聚合
        const files = await cluster.aggregateList(url.searchParams.get('prefix') || '');
        const xml = generateXmlList(files, 'virtual-bucket');
        return new Response(xml, { headers: { 'Content-Type': 'application/xml' } });
      }

      // === GET / HEAD OBJECT (DOWNLOAD) ===
      if (request.method === 'GET' || request.method === 'HEAD') {
        // 1. 检查 Edge Cache (仅 GET)
        if (request.method === 'GET') {
          const cachedRes = await cache.match(request);
          if (cachedRes) return cachedRes;
        }

        // 2. 查 KV 索引定位文件
        const fileInfo = await cluster.locateFile(key);
        
        if (!fileInfo) return new Response('Not Found', { status: 404 });

        // HEAD 请求直接构造响应
        if (request.method === 'HEAD') {
            const headers = new Headers();
            headers.set('Content-Length', fileInfo.size.toString());
            headers.set('Content-Type', 'application/octet-stream'); // 简化处理，索引未存 content-type
            headers.set('Last-Modified', new Date(fileInfo.lastModified).toUTCString());
            headers.set('ETag', `"${fileInfo.etag}"`);
            headers.set('X-Served-By', fileInfo.bucket);
            return new Response(null, { status: 200, headers });
        }

        // 3. 回源下载
        const response = await cluster.getObject(fileInfo.bucket, key, request.headers.get('range') || undefined);
        
        const newHeaders = new Headers(response.headers);
        newHeaders.set('X-Served-By', fileInfo.bucket);
        
        // 4. 写入 Edge Cache (异步)
        // 只有 200 OK 才缓存，206 Partial Content Cloudflare 默认不支持通过 Cache API 缓存
        if (response.status === 200) {
           ctx.waitUntil(cache.put(request, response.clone()));
        }
        
        return new Response(response.body, {
          status: response.status,
          statusText: response.statusText,
          headers: newHeaders
        });
      }

      // === PUT OBJECT (UPLOAD) ===
      if (request.method === 'PUT') {
        // 1. 检查是否已存在 (用于删除旧文件，如果旧文件在不同桶)
        const existing = await cluster.locateFile(key);
        
        // 2. 读取大小
        const size = parseInt(request.headers.get('Content-Length') || '0');
        
        // 3. 选桶 (基于 KV 索引计算容量)
        const targetBucket = await cluster.selectBucketForUpload(size);

        // 修改点：如果找不到可用桶，返回 507 错误
        if (!targetBucket) {
          return new Response('Insufficient Storage: No bucket has enough space for this file.', { status: 507 });
        }

        // 4. 上传 (内部会自动更新 KV 索引)
        const backendRes = await cluster.putObject(targetBucket, key, request.body, request.headers);

        // 5. 如果之前文件在另一个桶，删掉旧的 (内部会自动更新 KV 索引)
        if (existing && existing.bucket !== targetBucket) {
           ctx.waitUntil(cluster.deleteObject(existing.bucket, key));
        }

        const respHeaders = new Headers();
        const etag = backendRes.headers.get('ETag');
        if (etag) respHeaders.set('ETag', etag);

        return new Response(null, { status: 200, headers: respHeaders });
      }

      // === DELETE OBJECT ===
      if (request.method === 'DELETE') {
        const existing = await cluster.locateFile(key);
        if (existing) {
          // 内部会自动更新 KV 索引
          await cluster.deleteObject(existing.bucket, key);
        }
        return new Response(null, { status: 204 });
      }

      return new Response('Method Not Allowed', { status: 405 });

    } catch (e: any) {
      console.error('Worker Error:', e);
      return new Response(`Error: ${e.message}`, { status: 500 });
    }
  }
};