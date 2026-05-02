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

const onlineUsers = new Map();

io.on('connection', (socket) => {
  console.log('✅ Клиент подключен:', socket.id);
  
  let userPhone = null;

  socket.on('register', (data) => {
    userPhone = data.phone;
    onlineUsers.set(userPhone, socket.id);
    console.log(`📝 Зарегистрирован: ${userPhone}`);
    io.emit('user_status', { phone: userPhone, is_online: true });
  });

  socket.on('send_message', (data) => {
    console.log('📨 Сообщение от', data.from);
    
    if (data.type === 'private' && data.to) {
      const targetSocketId = onlineUsers.get(data.to);
      if (targetSocketId) {
        io.to(targetSocketId).emit('new_message', data);
      }
    }
    
    socket.emit('message_sent', { id: Date.now(), ...data });
  });

  socket.on('ping', () => {
    socket.emit('pong');
  });

  socket.on('disconnect', () => {
    console.log('❌ Отключен:', socket.id);
    if (userPhone) {
      onlineUsers.delete(userPhone);
      io.emit('user_status', { phone: userPhone, is_online: false });
    }
  });
});

app.get('/healthz', (req, res) => {
  res.status(200).json({ status: 'ok', connections: io.engine.clientsCount });
});

server.listen(PORT, () => {
  console.log(`🚀 Сервер запущен на порту ${PORT}`);
});
