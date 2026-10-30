#!/bin/bash
# Установка игрового сервера Nature Walk на чистый сервер (Ubuntu 22.04 / 24.04 или Debian 12).
# Запускать от root одной командой (см. README, раздел «Российский сервер»):
#   curl -fsSL https://raw.githubusercontent.com/parhomenkodd20-lang/game/main/deploy/setup.sh | bash
# Скрипт можно запускать повторно: он ничего не ломает, а просто обновляет игру.
set -e
REPO="https://github.com/parhomenkodd20-lang/game.git"
DIR=/opt/game
PORT=${PORT:-80}

if [ "$(id -u)" != "0" ]; then echo "Запусти от root (на сервере обычно уже root)."; exit 1; fi
export DEBIAN_FRONTEND=noninteractive

echo "==> 1/6 Ставлю нужные программы"
apt-get update -y
apt-get install -y git curl ca-certificates ufw

echo "==> 2/6 Ставлю Node.js (нужна версия 18 или новее)"
need_node=1
if command -v node >/dev/null 2>&1; then
  v=$(node -v | sed 's/^v//; s/\..*//')
  if [ "$v" -ge 18 ]; then need_node=0; fi
fi
if [ "$need_node" = "1" ]; then
  apt-get install -y nodejs npm || true
  v=0; command -v node >/dev/null 2>&1 && v=$(node -v | sed 's/^v//; s/\..*//')
  if [ "$v" -lt 18 ]; then
    echo "В системе слишком старый Node.js, ставлю свежий с nodesource.com"
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
    apt-get install -y nodejs
  fi
fi
echo "Node.js: $(node -v)"

echo "==> 3/6 Скачиваю игру с GitHub"
if [ -d "$DIR/.git" ]; then
  git -C "$DIR" pull --ff-only
else
  git clone "$REPO" "$DIR"
fi
mkdir -p "$DIR/data"

echo "==> 4/6 Создаю службу, которая сама запускает сервер и перезапускает его после сбоев и перезагрузки"
id game >/dev/null 2>&1 || useradd --system --home "$DIR" --shell /usr/sbin/nologin game
chown -R game:game "$DIR/data"
cat > /etc/systemd/system/nature-walk.service <<EOF
[Unit]
Description=Nature Walk game server
After=network.target

[Service]
User=game
WorkingDirectory=$DIR
Environment=PORT=$PORT
Environment=DATA_DIR=$DIR/data
EnvironmentFile=-/etc/nature-walk.env
ExecStart=$(command -v node) server.js
Restart=always
RestartSec=2
AmbientCapabilities=CAP_NET_BIND_SERVICE
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable nature-walk >/dev/null 2>&1
systemctl restart nature-walk

echo "==> 5/6 Включаю автообновление: раз в 5 минут сервер сам забирает новую версию с GitHub"
cat > /usr/local/bin/nature-walk-update <<EOF
#!/bin/bash
cd $DIR || exit 1
old=\$(git rev-parse HEAD)
git pull --ff-only -q || exit 0
new=\$(git rev-parse HEAD)
if [ "\$old" != "\$new" ]; then systemctl restart nature-walk; fi
EOF
chmod +x /usr/local/bin/nature-walk-update
echo "*/5 * * * * root /usr/local/bin/nature-walk-update >/dev/null 2>&1" > /etc/cron.d/nature-walk-update

echo "==> 6/6 Открываю порт $PORT (и 22 для входа на сервер)"
ufw allow 22/tcp >/dev/null 2>&1 || true
ufw allow "$PORT"/tcp >/dev/null 2>&1 || true
ufw --force enable >/dev/null 2>&1 || true

sleep 2
IP=$(curl -4 -fsS --max-time 8 https://api.ipify.org 2>/dev/null || hostname -I | awk '{print $1}')
echo
if systemctl is-active --quiet nature-walk; then
  echo "ГОТОВО. Сервер запущен."
  if [ "$PORT" = "80" ]; then echo "Адрес игры:   http://$IP/"; echo "Проверка:     http://$IP/health"; else echo "Адрес игры:   http://$IP:$PORT/"; echo "Проверка:     http://$IP:$PORT/health"; fi
else
  echo "Сервер не запустился. Покажи мне вывод команды:  journalctl -u nature-walk -n 40 --no-pager"
fi
