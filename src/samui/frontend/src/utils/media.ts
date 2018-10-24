import enquire from 'enquire.js';

if (typeof window !== 'undefined') {
  const matchMediaPolyfill = (mediaQuery: string) => {
    return {
      Media: mediaQuery,
      matches: false,
      addListener() {},
      removeListener() {},
    };
  };
  window.matchMedia = window.matchMedia || matchMediaPolyfill;
}

const mobileQuery = 'only screen and (max-width: 767.99px)';

export function enquireScreen(callback: (isMatch: boolean) => void, query: string = mobileQuery) {
  if (!enquire) {
    return;
  }

  const handler = {
    match: () => {
      if (callback) {
        callback(true);
      }
    },
    unmatch: () => {
      if (callback) {
        callback(false);
      }
    },
  };
  enquire.register(query, handler);
  return handler;
}

export function unenquireScreen(handler: any, query: string = mobileQuery) {
  if (!enquire) {
    return;
  }
  enquire.unregister(query, handler);
}
