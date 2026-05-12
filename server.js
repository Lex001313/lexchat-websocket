/////////////////////////////////////
// Project: LexChat                 /
// Author: R.I.Moskalenko (Lex0013) /
// License: MIT                     /
// Copyright (c) 2026               /
/////////////////////////////////////
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

// ========== ХРАНИЛИЩА ==========
const onlineUsers = new Map();        // phone -> socket.id
const userNames = new Map();          // phone -> name
const userSockets = new Map();        // socket.id -> phone
const groupMembers = new Map();       // group_id -> Set(phones)

// ========== СТАТИСТИКА ==========
const stats = {
  totalConnections: 0,
  messagesProcessed: 0,
  commands: {},
  startTime: Date.now()
};

function log(type, message, data = null) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [${type}] ${message}`);
  if (data) console.log(`  └─ ${JSON.stringify(data)}`);
  if (type === 'COMMAND') {
    const cmd = message.split(' ')[0];
    stats.commands[cmd] = (stats.commands[cmd] || 0) + 1;
    stats.messagesProcessed++;
  }
}

// ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
function broadcastToAll(event, data, excludePhone = null) {
  let sent = 0;
  onlineUsers.forEach((socketId, phone) => {
    if (excludePhone !== phone) {
      const socket = io.sockets.sockets.get(socketId);
      if (socket) {
        socket.emit(event, data);
        sent++;
      }
    }
  });
  log('BROADCAST', `${event} -> ${sent} clients`, data);
  return sent;
}

function sendToUser(phone, event, data) {
  const socketId = onlineUsers.get(phone);
  if (socketId) {
    const socket = io.sockets.sockets.get(socketId);
    if (socket) {
      socket.emit(event, data);
      log('SEND', `${event} -> ${phone}`, data);
      return true;
    }
  }
  log('SEND', `${event} -> ${phone} (offline)`, data);
  return false;
}

// ========== HEALTH CHECK ==========
app.get('/healthz', (req, res) => {
  res.status(200).json({ 
    status: 'ok', 
    connections: io.engine.clientsCount,
    online: onlineUsers.size,
    uptime: Math.floor((Date.now() - stats.startTime) / 1000),
    stats: {
      totalMessages: stats.messagesProcessed,
      commands: stats.commands,
      totalConnections: stats.totalConnections
    }
  });
});

app.get('/stats', (req, res) => {
  res.json({
    online: onlineUsers.size,
    connections: io.engine.clientsCount,
    uptime: Math.floor((Date.now() - stats.startTime) / 1000),
    commands: stats.commands,
    users: Array.from(onlineUsers.entries()).map(([phone, sid]) => ({
      phone,
      name: userNames.get(phone),
      socketId: sid
    }))
  });
});

// ========== HTTP ЭНДПОИНТЫ ==========
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.post('/api/broadcast', (req, res) => {
  const { broadcast_id, message, from_phone } = req.body;
  if (!broadcast_id || !message) {
    return res.status(400).json({ error: 'Missing broadcast_id or message' });
  }
  const sent = broadcastToAll('new_broadcast', {
    broadcast_id: parseInt(broadcast_id),
    message: message,
    from_phone: from_phone || 'admin'
  });
  res.json({ success: true, sent: sent });
});

app.post('/api/polling_settings', (req, res) => {
  const settings = req.body;
  broadcastToAll('new_polling_settings', {
    chats_poll_interval: settings.chats_poll_interval,
    broadcast_poll_interval: settings.broadcast_poll_interval,
    messages_poll_interval_fallback: settings.messages_poll_interval_fallback,
    disable_groups: settings.disable_groups
  });
  res.json({ success: true });
});

app.post('/api/colors', (req, res) => {
  const { colors } = req.body;
  broadcastToAll('new_colors', { colors: colors });
  res.json({ success: true });
});

app.post('/api/groups_toggle', (req, res) => {
  const { enabled } = req.body;
  broadcastToAll('groups_toggle', { enabled: enabled === true || enabled === 1 });
  res.json({ success: true });
});

app.post('/api/avatar_update', (req, res) => {
  const { phone, avatar } = req.body;
  broadcastToAll('new_avatar', { phone: phone, avatar: avatar }, phone);
  res.json({ success: true });
});

app.post('/api/chats_update', (req, res) => {
  const { to, reason } = req.body;
  if (to) {
    sendToUser(to, 'new_chats', { reason: reason || 'update' });
  } else {
    broadcastToAll('new_chats', { reason: reason || 'update' });
  }
  res.json({ success: true });
});

app.post('/api/calls_settings', (req, res) => {
  const settings = req.body;
  broadcastToAll('calls_settings_updated', settings);
  res.json({ success: true });
});

// ========== SOCKET.IO ==========
io.on('connection', (socket) => {
  stats.totalConnections++;
  log('CONNECT', `Client ${socket.id} connected (total: ${onlineUsers.size})`);
  
  let userPhone = null;
  let userName = null;

  socket.on('register', (data) => {
    userPhone = data.phone;
    userName = data.name || data.phone;
    
    const oldSocketId = onlineUsers.get(userPhone);
    if (oldSocketId && oldSocketId !== socket.id) {
      const oldSocket = io.sockets.sockets.get(oldSocketId);
      if (oldSocket) oldSocket.disconnect(true);
    }
    
    onlineUsers.set(userPhone, socket.id);
    userNames.set(userPhone, userName);
    userSockets.set(socket.id, userPhone);
    
    socket.emit('registered', { success: true, phone: userPhone, name: userName });
    broadcastToAll('user_status', { phone: userPhone, name: userName, is_online: true }, userPhone);
  });

  socket.on('send_message', (data) => {
    log('COMMAND', `send_message from ${data.from} to ${data.to}`);
    const messageData = {
      type: 'private',
      from: data.from,
      from_name: userNames.get(data.from) || data.from,
      text: data.text || null,
      msg_id: data.msg_id || Date.now(),
      time: Math.floor(Date.now() / 1000),
      file: data.file || null,
      audio: data.audio || null,
      data: data.data || null
    };
    const sent = sendToUser(data.to, 'new_message', messageData);
    socket.emit('message_sent', { success: true, msg_id: data.msg_id, delivered: sent });
  });

  socket.on('send_group_message', (data) => {
    log('COMMAND', `send_group_message from ${data.from} to group ${data.group_id}`);
    const messageData = {
      type: 'group',
      group_id: data.group_id,
      from: data.from,
      from_name: userNames.get(data.from) || data.from,
      text: data.text || null,
      msg_id: data.msg_id || Date.now(),
      time: Math.floor(Date.now() / 1000),
      file: data.file || null,
      audio: data.audio || null,
      data: data.data || null
    };
    io.to(`group_${data.group_id}`).emit('new_message', messageData);
    socket.emit('message_sent', { success: true, msg_id: data.msg_id, group_id: data.group_id });
  });
  
  socket.on('delete_message', (data) => {
    log('COMMAND', `delete_message ${data.msg_id} from ${data.from}`);
    if (data.type === 'private') {
      sendToUser(data.to, 'delete_message', { msg_id: data.msg_id, from: data.from });
    } else if (data.type === 'group') {
      socket.to(`group_${data.group_id}`).emit('delete_message', {
        msg_id: data.msg_id, from: data.from, group_id: data.group_id
      });
    }
    socket.emit('message_deleted', { success: true, msg_id: data.msg_id });
  });

  socket.on('typing_start', (data) => {
    sendToUser(data.to, 'user_typing', { from: data.from, from_name: userNames.get(data.from) || data.from, is_typing: true });
  });

  socket.on('typing_stop', (data) => {
    sendToUser(data.to, 'user_typing', { from: data.from, from_name: userNames.get(data.from) || data.from, is_typing: false });
  });

  socket.on('group_typing_start', (data) => {
    socket.to(`group_${data.group_id}`).emit('group_typing', {
      from: data.from, from_name: userNames.get(data.from) || data.from, group_id: data.group_id, is_typing: true
    });
  });

  socket.on('group_typing_stop', (data) => {
    socket.to(`group_${data.group_id}`).emit('group_typing', {
      from: data.from, from_name: userNames.get(data.from) || data.from, group_id: data.group_id, is_typing: false
    });
  });

  socket.on('new_broadcast', (data) => {
    broadcastToAll('new_broadcast', { broadcast_id: data.broadcast_id, message: data.message, from_phone: data.from_phone });
  });

  socket.on('new_polling_settings', (data) => {
    broadcastToAll('new_polling_settings', {
      chats_poll_interval: data.chats_poll_interval,
      broadcast_poll_interval: data.broadcast_poll_interval,
      messages_poll_interval_fallback: data.messages_poll_interval_fallback,
      disable_groups: data.disable_groups
    });
  });

  socket.on('new_colors', (data) => {
    broadcastToAll('new_colors', { colors: data.colors });
  });

  socket.on('new_avatar', (data) => {
    broadcastToAll('new_avatar', { phone: data.phone, avatar: data.avatar }, data.phone);
  });

  socket.on('groups_toggle', (data) => {
    broadcastToAll('groups_toggle', { enabled: data.enabled });
  });

  socket.on('new_chats', (data) => {
    if (data.to) {
      sendToUser(data.to, 'new_chats', { reason: data.reason || 'update' });
    } else {
      broadcastToAll('new_chats', { reason: data.reason || 'update' });
    }
  });

  socket.on('get_status', (data) => {
    const isOnline = onlineUsers.has(data.phone);
    socket.emit('user_status', { phone: data.phone, name: userNames.get(data.phone) || data.phone, is_online: isOnline });
  });

  socket.on('join_group', (data) => {
    const roomName = `group_${data.group_id}`;
    socket.join(roomName);
    if (!groupMembers.has(data.group_id)) groupMembers.set(data.group_id, new Set());
    if (userPhone) groupMembers.get(data.group_id).add(userPhone);
  });

  socket.on('leave_group', (data) => {
    const roomName = `group_${data.group_id}`;
    socket.leave(roomName);
    if (userPhone && groupMembers.has(data.group_id)) groupMembers.get(data.group_id).delete(userPhone);
  });
  
  socket.on('ping', () => { socket.emit('pong'); });
  socket.on('pong', () => {});

  socket.on('disconnect', (reason) => {
    log('DISCONNECT', `${userPhone || socket.id} disconnected. Reason: ${reason}`);
    if (userPhone) {
      onlineUsers.delete(userPhone);
      userSockets.delete(socket.id);
      broadcastToAll('user_status', { phone: userPhone, name: userName, is_online: false });
    }
  });
  
  // ========== ЗВОНКИ: СИГНАЛИНГ (push отправляется из PHP, не из Node.js) ==========
  socket.on('call_start', (data) => {
    const { to, type } = data;
    const fromPhone = userPhone || socket.id;
    log('CALL', `start ${type} call from ${fromPhone} to ${to}`);
    sendToUser(to, 'incoming_call', {
      from: fromPhone,
      from_name: userNames.get(fromPhone) || fromPhone,
      type: type
    });
  });

  socket.on('call_offer', (data) => {
    sendToUser(data.to, 'call_offer', { from: userPhone, offer: data.offer });
  });

  socket.on('call_answer', (data) => {
    sendToUser(data.to, 'call_answer', { from: userPhone, answer: data.answer });
  });

  socket.on('call_ice', (data) => {
    sendToUser(data.to, 'call_ice', { from: userPhone, candidate: data.candidate });
  });

  socket.on('call_hangup', (data) => {
    sendToUser(data.to, 'call_hangup', { from: userPhone });
  });
});

// ========== ЗАПУСК ==========
server.listen(PORT, () => {
  console.log(`
╔═══════════════════════════════════════════════════════════════════════════════╗
║                    🚀 LexChat WebSocket Server v2.1 (Socket.IO)               ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║  HTTP Server:  http://localhost:${PORT}                                         ║
║  Health check: http://localhost:${PORT}/healthz                                 ║
║  Stats:         http://localhost:${PORT}/stats                                  ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║  Статус:         ✅ Сервер запущен                                             ║
║  Порт:           ${PORT}                                                         ║
║  WebSocket:      ✅ готов к подключениям                                       ║
╚═══════════════════════════════════════════════════════════════════════════════╝
  `);
});
