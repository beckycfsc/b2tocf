// src/core/config.ts

export interface Env {
  // 基础配置
  BUCKETS_CONFIG: string; // 格式: name:id:key:endpoint|...
  VIRTUAL_ACCESS_KEY_ID: string;
  VIRTUAL_SECRET_ACCESS_KEY: string;
  
  // 策略与限制
  UPLOAD_STRATEGY: 'fill-first' | 'balanced'; // 默认 fill-first
  MAX_BUCKET_SIZE_GB: string; // 默认 10
  
  // 缓存 TTL (秒)
  CACHE_TTL_METADATA: string; // 默认 3600 (路径->桶映射)
  CACHE_TTL_LIST: string;     // 默认 600 (文件列表)
  ENABLE_CONTENT_CACHE: string; // "true" / "false"

  // Bindings
  BUCKET_STATE_KV: KVNamespace;
}

export interface BucketConfig {
  name: string;
  accessKeyId: string;
  secretAccessKey: string;
  endpoint: string;
  region: string;
}

export function parseBucketsConfig(configStr: string): BucketConfig[] {
  if (!configStr) return [];
  
  // 去除空白符并按 | 分割
  return configStr.split('|')
    .map(s => s.trim())
    .filter(s => s.length > 0)
    .map(entry => {
      const parts = entry.split(':');
      if (parts.length < 4) {
        console.warn(`Invalid bucket config format: ${entry}`);
        return null;
      }
      // 假设 endpoint 格式为 s3.us-west-004.backblazeb2.com
      // region 通常是 endpoint 的第二部分
      const endpoint = parts[3];
      const region = endpoint.split('.')[1] || 'us-east-1';

      return {
        name: parts[0],
        accessKeyId: parts[1],
        secretAccessKey: parts[2],
        endpoint: `https://${endpoint}`,
        region: region
      };
    })
    .filter((b): b is BucketConfig => b !== null);
}