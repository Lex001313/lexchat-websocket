const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');

const app = express();
const server = createServer(app);
const PORT = process.env.PORT || 10000;

// Socket.IO с правильной настройкой для Render
const io = new Server(server, {
  cors: {
    origin: "*", // Настройте под свой домен
    methods: ["GET", "POST"],
    credentials: true
  },
  // Важные настройки для стабильности на Render [citation:1]
  pingTimeout: 60000,
  pingInterval: 25000,
  transports: ['websocket', 'polling'],
  allowEIO3: true,
  // Включение восстановления состояния соединения [citation:8]
  connectionStateRecovery: {
    maxDisconnectionDuration: 2 * 60 * 1000, // 2 минуты
    skipMiddlewares: true,
  }
});

io.on('connection', (socket) => {
  console.log('Клиент подключен:', socket.id);
  
  // Информация о восстановлении
  if (socket.recovered) {
    console.log('✅ Соединение восстановлено');
  }

  socket.on('message', (data) => {
    console.log('Получено:', data);
    io.emit('message', data);
  });

  socket.on('disconnect', (reason) => {
    console.log('Клиент отключен:', socket.id, reason);
  });
});

// Health check для Render
app.get('/healthz', (req, res) => {
  res.status(200).json({ status: 'ok' });
});

server.listen(PORT, () => {
  console.log(`Сервер запущен на порту ${PORT}`);
});
