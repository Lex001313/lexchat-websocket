const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');

const app = express();
const server = createServer(app);
const PORT = process.env.PORT || 10000;

// ========== ХРАНИЛИЩА ==========
const onlineUsers = new Map();        // phone -> { socketId, name, lastSeen }
const userSockets = new Map();        // socketId -> phone

// Настройки
const HEARTBEAT_INTERVAL = 25000;
const INACTIVE_TIMEOUT = 35000;

// Статистика
let messageCount = 0;
let lastStatsReset = Date.now();

// Текущая тема (кэш)
let currentThemeSettings = {
  colors: {
    dark_bg: '#0a0f12',
    dark_sidebar_bg: '#111b21',
    dark_header_bg: '#202c33',
    dark_text: '#e9edef',
    dark_message_in_bg: '#202c33',
    dark_message_out_bg: '#005c4b',
    dark_input_bg: '#2a3942',
    light_bg: '#ffffff',
    light_sidebar_bg: '#ffffff',
    light_header_bg: '#e9edef',
    light_text: '#111b21',
    light_message_in_bg: '#ffffff',
    light_message_out_bg: '#d9fdd3',
    light_input_bg: '#ffffff'
  },
  background_url: '/fonDefault.png'
};

// ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

// Уведомление всех о новом сообщении
function notifyNewMessage(data) {
  if (data.type === 'private' && data.to) {
    const target = onlineUsers.get(data.to);
    if (target) {
      io.to(target.socketId).emit('new_message', data);
    }
  }
  if (data.type === 'group' && data.group_id) {
    io.to(`group_${data.group_id}`).emit('new_message', data);
  }
  // Обновляем чаты у отправителя
  const sender = onlineUsers.get(data.from);
  if (sender) {
    io.to(sender.socketId).emit('chats_update');
  }
}

// Уведомление о статусе онлайн
function notifyUserStatus(phone, name, isOnline) {
  io.emit('user_status', { phone, name, is_online: isOnline });
}

// Уведомление об обновлении темы
function notifyThemeChanged() {
  io.emit('theme_changed', {
    colors: currentThemeSettings.colors,
    background_url: currentThemeSettings.background_url
  });
}

// ========== WEBSOCKET СЕРВЕР ==========
const io = new Server(server, {
  cors: { 
    origin: "*", 
    methods: ["GET", "POST"],
    credentials: true
  },
  pingTimeout: 60000,
  pingInterval: 25000,
  transports: ['websocket', 'polling']
});

io.on('connection', (socket) => {
  console.log('✅ Клиент подключен:', socket.id);
  let userPhone = null;
  let userName = null;
  
  // Heartbeat интервал
  const heartbeatInterval = setInterval(() => {
    if (userPhone && onlineUsers.has(userPhone)) {
      const user = onlineUsers.get(userPhone);
      user.lastSeen = Date.now();
      onlineUsers.set(userPhone, user);
    }
    socket.emit('heartbeat', { timestamp: Date.now() });
  }, HEARTBEAT_INTERVAL);
  
  // ========== РЕГИСТРАЦИЯ ==========
  socket.on('register', (data) => {
    userPhone = data.phone;
    userName = data.name || data.phone;
    
    onlineUsers.set(userPhone, {
      socketId: socket.id,
      name: userName,
      lastSeen: Date.now()
    });
    userSockets.set(socket.id, userPhone);
    
    console.log(`📝 Зарегистрирован: ${userName} (${userPhone})`);
    console.log(`👥 Онлайн: ${onlineUsers.size}`);
    
    // Отправляем текущую тему
    socket.emit('theme_settings', {
      colors: currentThemeSettings.colors,
      background_url: currentThemeSettings.background_url
    });
    
    // Сообщаем всем о новом онлайн пользователе
    notifyUserStatus(userPhone, userName, true);
  });
  
  // ========== СТАТУС ПЕЧАТИ ==========
  socket.on('typing_start', (data) => {
    if (!userPhone) return;
    if (data.to) {
      const target = onlineUsers.get(data.to);
      if (target) {
        io.to(target.socketId).emit('user_typing', {
          from: userPhone,
          from_name: userName,
          is_typing: true
        });
      }
    }
    if (data.group_id) {
      socket.to(`group_${data.group_id}`).emit('group_typing', {
        from: userPhone,
        from_name: userName,
        group_id: data.group_id,
        is_typing: true
      });
    }
  });
  
  socket.on('typing_stop', (data) => {
    if (!userPhone) return;
    if (data.to) {
      const target = onlineUsers.get(data.to);
      if (target) {
        io.to(target.socketId).emit('user_typing', {
          from: userPhone,
          from_name: userName,
          is_typing: false
        });
      }
    }
    if (data.group_id) {
      socket.to(`group_${data.group_id}`).emit('group_typing', {
        from: userPhone,
        from_name: userName,
        group_id: data.group_id,
        is_typing: false
      });
    }
  });
  
  // ========== ВХОД/ВЫХОД ИЗ ГРУПП ==========
  socket.on('join_group', (data) => {
    if (!userPhone || !data.group_id) return;
    socket.join(`group_${data.group_id}`);
    console.log(`👥 ${userPhone} присоединился к группе ${data.group_id}`);
  });
  
  socket.on('leave_group', (data) => {
    if (!data.group_id) return;
    socket.leave(`group_${data.group_id}`);
    console.log(`👋 ${userPhone} покинул группу ${data.group_id}`);
  });
  
  // ========== PING/PONG ==========
  socket.on('ping', () => {
    socket.emit('pong', { timestamp: Date.now() });
  });
  
  // ========== ОТКЛЮЧЕНИЕ ==========
  socket.on('disconnect', async () => {
    console.log('❌ Клиент отключен:', socket.id);
    clearInterval(heartbeatInterval);
    
    const phone = userSockets.get(socket.id);
    if (phone) {
      onlineUsers.delete(phone);
      userSockets.delete(socket.id);
      notifyUserStatus(phone, userName, false);
      console.log(`👤 ${phone} вышел, онлайн: ${onlineUsers.size}`);
    }
  });
});

// ========== ФОНОВЫЕ ЗАДАЧИ ==========

// Проверка неактивных пользователей
setInterval(() => {
  const now = Date.now();
  for (const [phone, user] of onlineUsers.entries()) {
    if (now - user.lastSeen > INACTIVE_TIMEOUT) {
      console.log(`⏰ ${phone} неактивен, отмечаем оффлайн`);
      onlineUsers.delete(phone);
      notifyUserStatus(phone, user.name, false);
    }
  }
}, 30000);

// Статистика
setInterval(() => {
  const now = Date.now();
  const minutes = (now - lastStatsReset) / 1000 / 60;
  const messagesPerMinute = minutes > 0 ? Math.round(messageCount / minutes) : 0;
  
  const stats = {
    online: onlineUsers.size,
    messagesPerMinute: messagesPerMinute,
    memoryUsage: Math.round(process.memoryUsage().rss / 1024 / 1024),
    uptime: Math.floor(process.uptime())
  };
  
  console.log('📊 Статистика:', stats);
  
  messageCount = 0;
  lastStatsReset = now;
}, 60000);

// ========== HTTP ENDPOINTS ==========

// Health check
app.get('/healthz', (req, res) => {
  res.json({ 
    status: 'ok', 
    connections: io.engine.clientsCount,
    online: onlineUsers.size,
    uptime: process.uptime()
  });
});

// Статистика
app.get('/stats', (req, res) => {
  res.json({
    online: onlineUsers.size,
    connections: io.engine.clientsCount,
    memory_usage_mb: Math.round(process.memoryUsage().rss / 1024 / 1024),
    uptime_seconds: Math.floor(process.uptime())
  });
});

// Текущая тема
app.get('/theme', (req, res) => {
  res.json({
    colors: currentThemeSettings.colors,
    background_url: currentThemeSettings.background_url
  });
});

// ========== WEBHOOKS ДЛЯ УВЕДОМЛЕНИЙ ОТ PHP ==========

// Новое сообщение
app.post('/notify/message', express.json(), (req, res) => {
  const data = req.body;
  console.log('📨 Webhook: новое сообщение', data);
  messageCount++;
  notifyNewMessage(data);
  res.json({ success: true });
});

// Обновление статуса онлайн
app.post('/notify/status', express.json(), (req, res) => {
  const { phone, name, is_online } = req.body;
  console.log(`🟢 Webhook: статус ${phone} -> ${is_online ? 'онлайн' : 'оффлайн'}`);
  
  if (is_online) {
    // Не обновляем onlineUsers здесь, т.к. пользователь должен сам зарегистрироваться
  }
  notifyUserStatus(phone, name, is_online);
  res.json({ success: true });
});

// Обновление темы/фона
app.post('/notify/theme', express.json(), (req, res) => {
  const { colors, background_url } = req.body;
  console.log('🎨 Webhook: обновление темы');
  
  if (colors) {
    currentThemeSettings.colors = colors;
  }
  if (background_url) {
    currentThemeSettings.background_url = background_url;
  }
  
  notifyThemeChanged();
  res.json({ success: true });
});

// Старт сервера
server.listen(PORT, () => {
  console.log(`\n🚀 WebSocket сервер запущен на порту ${PORT}`);
  console.log(`📡 Health check: https://lexchat-websocket.onrender.com/healthz`);
  console.log(`📊 Статистика: https://lexchat-websocket.onrender.com/stats`);
  console.log(`🎨 Тема: https://lexchat-websocket.onrender.com/theme`);
  console.log(`\n⚡ Готов к работе!\n`);
});
