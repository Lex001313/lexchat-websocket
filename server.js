const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');

const app = express();
const server = createServer(app);
const PORT = process.env.PORT || 10000;

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
    credentials: true
  },
  pingTimeout: 60000,
  pingInterval: 25000,
  transports: ['websocket', 'polling'],
  connectionStateRecovery: {
    maxDisconnectionDuration: 2 * 60 * 1000,
    skipMiddlewares: true,
  }
});

// Хранилища
const onlineUsers = new Map();        // phone -> socket.id
const userNames = new Map();           // phone -> name
const typingUsers = new Map();         // phone -> { to, timeout }

io.on('connection', (socket) => {
  console.log('✅ Клиент подключен:', socket.id);
  
  let userPhone = null;
  let userName = null;

  // Регистрация пользователя
  socket.on('register', (data) => {
    userPhone = data.phone;
    userName = data.name || data.phone;
    userNames.set(userPhone, userName);
    onlineUsers.set(userPhone, socket.id);
    
    console.log(`📝 Зарегистрирован: ${userName} (${userPhone})`);
    console.log(`👥 Онлайн: ${onlineUsers.size} пользователей`);
    
    // Рассылаем всем обновлённый статус
    io.emit('user_status', { phone: userPhone, name: userName, is_online: true });
  });

  // Отправка сообщения
  socket.on('send_message', (data) => {
    console.log(`📨 Сообщение от ${data.from} к ${data.to || 'group_' + data.group_id}`);
    
    if (data.type === 'private' && data.to) {
      const targetSocketId = onlineUsers.get(data.to);
      if (targetSocketId) {
        io.to(targetSocketId).emit('new_message', data);
      }
    }
    
    if (data.type === 'group' && data.group_id) {
      socket.to(`group_${data.group_id}`).emit('new_message', data);
    }
    
    socket.emit('message_sent', { id: Date.now(), ...data });
  });

  // ========== СТАТУС ПЕЧАТИ ==========
  socket.on('typing_start', (data) => {
    const targetSocketId = onlineUsers.get(data.to);
    if (targetSocketId) {
      io.to(targetSocketId).emit('user_typing', {
        from: data.from,
        from_name: userNames.get(data.from) || data.from,
        is_typing: true
      });
    }
  });

  socket.on('typing_stop', (data) => {
    const targetSocketId = onlineUsers.get(data.to);
    if (targetSocketId) {
      io.to(targetSocketId).emit('user_typing', {
        from: data.from,
        from_name: userNames.get(data.from) || data.from,
        is_typing: false
      });
    }
  });

  // ========== СТАТУС ПЕЧАТИ В ГРУППЕ ==========
  socket.on('group_typing_start', (data) => {
    socket.to(`group_${data.group_id}`).emit('group_typing', {
      from: data.from,
      from_name: userNames.get(data.from) || data.from,
      group_id: data.group_id,
      is_typing: true
    });
  });

  socket.on('group_typing_stop', (data) => {
    socket.to(`group_${data.group_id}`).emit('group_typing', {
      from: data.from,
      from_name: userNames.get(data.from) || data.from,
      group_id: data.group_id,
      is_typing: false
    });
  });

  // ========== ОБНОВЛЕНИЕ СТАТУСА ОНЛАЙН ==========
  socket.on('update_status', (data) => {
    if (data.is_online) {
      onlineUsers.set(userPhone, socket.id);
    } else {
      onlineUsers.delete(userPhone);
    }
    io.emit('user_status', { 
      phone: userPhone, 
      name: userNames.get(userPhone) || userPhone,
      is_online: data.is_online 
    });
  });

  // Пинг
  socket.on('ping', () => {
    socket.emit('pong');
  });

  // Отключение
  socket.on('disconnect', () => {
    console.log('❌ Клиент отключен:', socket.id);
    if (userPhone) {
      onlineUsers.delete(userPhone);
      io.emit('user_status', { 
        phone: userPhone, 
        name: userNames.get(userPhone) || userPhone,
        is_online: false 
      });
    }
  });
});

// Health check
app.get('/healthz', (req, res) => {
  res.status(200).json({ status: 'ok', connections: io.engine.clientsCount });
});

// Статистика
app.get('/stats', (req, res) => {
  res.json({
    online: onlineUsers.size,
    connections: io.engine.clientsCount
  });
});

server.listen(PORT, () => {
  console.log(`🚀 Сервер запущен на порту ${PORT}`);
});
