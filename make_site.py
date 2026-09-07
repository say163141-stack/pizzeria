#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Собирает готовый САЙТ (PWA) из конвейера пиццерии в папку docs/ (для GitHub Pages).
1) запускает pipeline.py (свежие данные, оценка прогнозов);
2) оборачивает pizzeria_hero.html в полноценный HTML с манифестом PWA + service worker
   → docs/index.html (это устанавливаемое на телефон «приложение»).
Запуск (локально или в GitHub Actions):
   export ODDS_API_KEY=...   ;   python3 make_site.py
"""
import os, shutil, subprocess, sys

HEAD = '''<!doctype html><html lang="ru"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#070f1c">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="Пиццерия">
<link rel="manifest" href="manifest.webmanifest">
<link rel="apple-touch-icon" href="icon-192.png">
<link rel="icon" href="icon-192.png">
</head><body>
'''
TAIL = '''
<script>
if('serviceWorker' in navigator){
  var hadCtrl=!!navigator.serviceWorker.controller;
  navigator.serviceWorker.addEventListener('controllerchange',function(){
    if(hadCtrl&&!window.__reloaded){window.__reloaded=true;location.reload();}
  });
  window.addEventListener('load',function(){
    navigator.serviceWorker.register('sw.js').then(function(reg){reg.update&&reg.update();}).catch(function(){});
  });
}
</script>
</body></html>'''


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    os.chdir(here)
    # 1) конвейер (использует pipeline.py + шаблоны из этой же папки)
    subprocess.run([sys.executable, 'pipeline.py'], check=True)
    # 2) обёртка в полноценный документ
    body = open('pizzeria_hero.html', encoding='utf-8').read()
    os.makedirs('docs', exist_ok=True)
    open('docs/index.html', 'w', encoding='utf-8').write(HEAD + body + TAIL)
    for f in ('manifest.webmanifest', 'sw.js', 'icon-192.png', 'icon-512.png'):
        if os.path.exists(f):
            shutil.copy(f, 'docs/' + f)
    # .nojekyll — чтобы GitHub Pages не трогал файлы
    open('docs/.nojekyll', 'w').write('')
    print('Сайт собран в docs/  (index.html + PWA)')


if __name__ == '__main__':
    main()
