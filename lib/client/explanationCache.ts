/**
 * Client-side localStorage cache for explanations.
 * Reduces API calls for returning users and during a session.
 */

const CACHE_PREFIX = "dpv_explain_";
const RELATIONAL_CACHE_PREFIX = "dpv_rel_explain_";
const CACHE_VERSION = "v1";

interface CachedExplanation {
  explanation: string;
  timestamp: number;
  version: string;
}

/**
 * Get cache key for a node explanation
 */
function getKey(nodeId: string): string {
  return `${CACHE_PREFIX}${nodeId}`;
}

/**
 * Get cache key for a relational explanation
 */
function getRelationalKey(nodeId: string, anchorId: string): string {
  return `${RELATIONAL_CACHE_PREFIX}${nodeId}:${anchorId}`;
}

/**
 * Check if localStorage is available
 */
function isLocalStorageAvailable(): boolean {
  try {
    const test = "__test__";
    localStorage.setItem(test, test);
    localStorage.removeItem(test);
    return true;
  } catch {
    return false;
  }
}

/**
 * Get a cached explanation for a node
 */
export function getCachedExplanation(nodeId: string): string | null {
  if (!isLocalStorageAvailable()) return null;
  
  try {
    const cached = localStorage.getItem(getKey(nodeId));
    if (!cached) return null;
    
    const data: CachedExplanation = JSON.parse(cached);
    
    // Check version - invalidate old cache format
    if (data.version !== CACHE_VERSION) return null;
    
    return data.explanation;
  } catch {
    return null;
  }
}

/**
 * Cache an explanation for a node
 */
export function setCachedExplanation(nodeId: string, explanation: string): void {
  if (!isLocalStorageAvailable()) return;
  
  try {
    const data: CachedExplanation = {
      explanation,
      timestamp: Date.now(),
      version: CACHE_VERSION,
    };
    localStorage.setItem(getKey(nodeId), JSON.stringify(data));
  } catch (e) {
    // localStorage might be full - try to clear old entries
    console.warn("Failed to cache explanation:", e);
    clearOldEntries();
  }
}

/**
 * Get a cached relational explanation
 */
export function getCachedRelationalExplanation(nodeId: string, anchorId: string): string | null {
  if (!isLocalStorageAvailable()) return null;
  
  try {
    const cached = localStorage.getItem(getRelationalKey(nodeId, anchorId));
    if (!cached) return null;
    
    const data: CachedExplanation = JSON.parse(cached);
    if (data.version !== CACHE_VERSION) return null;
    
    return data.explanation;
  } catch {
    return null;
  }
}

/**
 * Cache a relational explanation
 */
export function setCachedRelationalExplanation(nodeId: string, anchorId: string, explanation: string): void {
  if (!isLocalStorageAvailable()) return;
  
  try {
    const data: CachedExplanation = {
      explanation,
      timestamp: Date.now(),
      version: CACHE_VERSION,
    };
    localStorage.setItem(getRelationalKey(nodeId, anchorId), JSON.stringify(data));
  } catch (e) {
    console.warn("Failed to cache relational explanation:", e);
    clearOldEntries();
  }
}

/**
 * Clear old cache entries to free up space
 * Removes entries older than 7 days
 */
function clearOldEntries(): void {
  if (!isLocalStorageAvailable()) return;
  
  const maxAge = 7 * 24 * 60 * 60 * 1000; // 7 days
  const now = Date.now();
  
  try {
    const keysToRemove: string[] = [];
    
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (!key?.startsWith(CACHE_PREFIX) && !key?.startsWith(RELATIONAL_CACHE_PREFIX)) continue;
      
      try {
        const cached = localStorage.getItem(key);
        if (!cached) continue;
        
        const data: CachedExplanation = JSON.parse(cached);
        if (now - data.timestamp > maxAge) {
          keysToRemove.push(key);
        }
      } catch {
        // Invalid entry, remove it
        keysToRemove.push(key);
      }
    }
    
    keysToRemove.forEach((key) => localStorage.removeItem(key));
    console.log(`[explanationCache] Cleared ${keysToRemove.length} old entries`);
  } catch {
    // Ignore errors
  }
}

/**
 * Clear all cached explanations
 */
export function clearAllCachedExplanations(): void {
  if (!isLocalStorageAvailable()) return;
  
  const keysToRemove: string[] = [];
  
  for (let i = 0; i < localStorage.length; i++) {
    const key = localStorage.key(i);
    if (key?.startsWith(CACHE_PREFIX) || key?.startsWith(RELATIONAL_CACHE_PREFIX)) {
      keysToRemove.push(key);
    }
  }
  
  keysToRemove.forEach((key) => localStorage.removeItem(key));
}

