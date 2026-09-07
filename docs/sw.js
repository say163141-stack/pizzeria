// Service worker: делает «приложение» устанавливаемым и открывает мгновенно.
// Стратегия: для страницы/данных — network БЕЗ HTTP-кэша (всегда свежие ставки),
// кэш только запасной (офлайн). Версия кэша меняется при апдейте → старое чистится.
const CACHE = 'pizzeria-v2';
const ASSETS = ['index.html', 'manifest.webmanifest', 'icon-192.png', 'icon-512.png'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(ASSETS)).then(() => self.skipWaiting()));
});
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k)))).then(() => self.clients.claim()));
});
self.addEventListener('fetch', e => {
  if (e.request.method !== 'GET') return;
  const req = e.request;
  const url = new URL(req.url);
  // страница (навигация) и index.html — тянем в обход HTTP-кэша, иначе GitHub Pages
  // отдаёт старую сборку до 10 минут (отсюда «прошедшие события» в приложении).
  const isDoc = req.mode === 'navigate' || /\/$|index\.html$/.test(url.pathname);
  const go = isDoc ? fetch(url.pathname + '?_=' + Date.now(), { cache: 'no-store' }) : fetch(req);
  e.respondWith(
    go.then(r => {
      const copy = r.clone();
      caches.open(CACHE).then(c => c.put(isDoc ? 'index.html' : req, copy)).catch(() => {});
      return r;
    }).catch(() => caches.match(isDoc ? 'index.html' : req).then(r => r || caches.match('index.html')))
  );
});
