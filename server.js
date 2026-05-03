const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');

const app = express();
const server = createServer(app);
const PORT = process.env.PORT || 10000;

// ========== КОНФИГУРАЦИЯ БД (InfinityFree) ==========
const dbConfig = {
  host: 'sql312.infinityfree.com',
  user: 'if0_41705858',
  password: 'Lex19910522',
  database: 'if0_41705858_baza',
  waitForConnections: true,
  connectionLimit: 10,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0
};

let pool = null;

// ========== ХРАНИЛИЩА ==========
const onlineUsers = new Map();        // phone -> { socketId, name, lastSeen }
const userSockets = new Map();        // socketId -> phone
const chatCache = new Map();          // phone -> { chats, timestamp }
const messageHistory = new Map();     // socketId -> последние 50 сообщений

// Настройки кэша
const CACHE_TTL = 30000;              // 30 секунд
const HEARTBEAT_INTERVAL = 25000;     // 25 секунд
const INACTIVE_TIMEOUT = 35000;       // 35 секунд без heartbeat -> оффлайн

// Статистика
let messageCount = 0;
let lastStatsReset = Date.now();

// ========== НАСТРОЙКИ ТЕМЫ (кэш в памяти) ==========
let currentThemeSettings = {
  theme: 'dark',
  last_update: Date.now(),
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

// ========== ПОДКЛЮЧЕНИЕ К БД ==========
async function initDB() {
  try {
    pool = await mysql.createPool(dbConfig);
    console.log('✅ MySQL подключена к InfinityFree');
    
    // Тестовый запрос
    const [result] = await pool.query('SELECT 1 as connected');
    if (result[0].connected === 1) {
      console.log('✅ MySQL проверка успешна');
    }
  } catch (error) {
    console.error('❌ Ошибка подключения к БД:', error.message);
    setTimeout(initDB, 10000);
  }
}
initDB();

// ========== ЗАГРУЗКА НАСТРОЕК ТЕМЫ ==========
async function loadThemeSettings() {
  try {
    // Пробуем загрузить из colors.json на PHP хостинге
    const https = require('https');
    const options = {
      hostname: 'lexchat.rf.gd',
      path: '/colors.json',
      method: 'GET',
      timeout: 5000
    };
    
    const req = https.get(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const colors = JSON.parse(data);
          if (colors && !colors.error) {
            currentThemeSettings.colors = colors;
            currentThemeSettings.last_update = Date.now();
            console.log('🎨 Настройки темы загружены из colors.json');
          }
        } catch(e) {
          console.log('⚠️ Не удалось распарсить colors.json');
        }
      });
    });
    
    req.on('error', (err) => {
      console.log('⚠️ Не удалось загрузить colors.json:', err.message);
    });
    
    req.end();
  } catch (error) {
    console.log('⚠️ Ошибка загрузки темы:', error.message);
  }
}

// Загружаем тему при старте
loadThemeSettings();

// Периодическая проверка обновлений темы (раз в 5 минут)
setInterval(loadThemeSettings, 300000);

// ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

// Получение списка чатов (с кэшем)
async function getChats(phone) {
  if (!phone) return [];
  
  // Проверяем кэш
  const cached = chatCache.get(phone);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    return cached.chats;
  }
  
  if (!pool) return [];
  
  try {
    const connection = await pool.getConnection();
    
    // 1. Личные чаты
    const [privateChats] = await connection.execute(`
      SELECT DISTINCT 
        CASE 
          WHEN from_phone = ? THEN to_phone 
          ELSE from_phone 
        END as contact_phone,
        MAX(time) as last_time
      FROM messages 
      WHERE (from_phone = ? OR to_phone = ?) AND deleted_at IS NULL
      GROUP BY contact_phone
    `, [phone, phone, phone]);
    
    const chats = [];
    
    for (const chat of privateChats) {
      const [users] = await connection.execute(
        'SELECT name, avatar, is_online FROM users WHERE phone = ?',
        [chat.contact_phone]
      );
      
      if (users.length === 0) continue;
      
      const [lastMsg] = await connection.execute(`
        SELECT text, file_path, audio_path, time, from_phone, status
        FROM messages 
        WHERE ((from_phone = ? AND to_phone = ?) OR (from_phone = ? AND to_phone = ?))
        AND deleted_at IS NULL
        ORDER BY time DESC LIMIT 1
      `, [phone, chat.contact_phone, chat.contact_phone, phone]);
      
      let lastMsgText = '';
      let lastTime = chat.last_time || 0;
      
      if (lastMsg.length > 0) {
        if (lastMsg[0].audio_path) lastMsgText = '🎤 Голосовое';
        else if (lastMsg[0].file_path) lastMsgText = '📎 Файл';
        else lastMsgText = (lastMsg[0].text || '').substring(0, 30);
        lastTime = lastMsg[0].time || lastTime;
      }
      
      const isOnline = onlineUsers.has(chat.contact_phone);
      const unreadCount = await getUnreadCount(connection, phone, chat.contact_phone);
      
      chats.push({
        id: chat.contact_phone,
        type: 'user',
        name: users[0].name,
        avatar: users[0].avatar || 'uploads/avatars/default.png',
        last_message: lastMsgText || 'Нет сообщений',
        last_time: lastTime,
        unread: unreadCount > 0,
        unread_count: unreadCount,
        is_online: isOnline
      });
    }
    
    // 2. Групповые чаты
    const [groups] = await connection.execute(`
      SELECT g.id, g.name, g.avatar, MAX(gmsg.time) as last_time
      FROM group_members gm
      JOIN \`groups\` g ON g.id = gm.group_id
      LEFT JOIN group_messages gmsg ON gmsg.group_id = g.id AND gmsg.deleted_at IS NULL
      WHERE gm.user_phone = ?
      GROUP BY g.id
    `, [phone]);
    
    for (const group of groups) {
      const [unreadResult] = await connection.execute(
        'SELECT COUNT(*) as cnt FROM group_messages WHERE group_id = ? AND from_phone != ? AND is_read = 0',
        [group.id, phone]
      );
      
      chats.push({
        id: group.id,
        type: 'group',
        name: group.name,
        avatar: group.avatar || 'uploads/group_avatars/default.png',
        last_message: 'Групповой чат',
        last_time: group.last_time || 0,
        unread: unreadResult[0].cnt > 0,
        unread_count: unreadResult[0].cnt,
        is_online: false
      });
    }
    
    // Сортировка по последнему времени
    chats.sort((a, b) => (b.last_time || 0) - (a.last_time || 0));
    
    connection.release();
    
    // Сохраняем в кэш
    chatCache.set(phone, { chats, timestamp: Date.now() });
    
    return chats;
  } catch (error) {
    console.error('Ошибка getChats:', error.message);
    return [];
  }
}

// Получение количества непрочитанных сообщений
async function getUnreadCount(connection, myPhone, contactPhone) {
  try {
    const [result] = await connection.execute(
      'SELECT COUNT(*) as cnt FROM messages WHERE from_phone = ? AND to_phone = ? AND (status != "read" OR is_read = 0) AND deleted_at IS NULL',
      [contactPhone, myPhone]
    );
    return result[0].cnt;
  } catch (error) {
    return 0;
  }
}

// Проверка активной рассылки
async function getActiveBroadcast(userPhone) {
  if (!pool) return null;
  
  try {
    const connection = await pool.getConnection();
    
    const [broadcast] = await connection.execute(
      'SELECT id, message, from_phone FROM broadcast_messages WHERE is_active = 1 ORDER BY id DESC LIMIT 1'
    );
    
    if (broadcast.length === 0) {
      connection.release();
      return null;
    }
    
    const [read] = await connection.execute(
      'SELECT id FROM user_broadcast_read WHERE user_phone = ? AND broadcast_id = ?',
      [userPhone, broadcast[0].id]
    );
    
    connection.release();
    
    if (read.length === 0) {
      return broadcast[0];
    }
    
    return null;
  } catch (error) {
    console.error('Ошибка getActiveBroadcast:', error.message);
    return null;
  }
}

// Пометить рассылку как прочитанную
async function markBroadcastRead(userPhone, broadcastId) {
  if (!pool) return false;
  
  try {
    const connection = await pool.getConnection();
    await connection.execute(
      'INSERT IGNORE INTO user_broadcast_read (user_phone, broadcast_id) VALUES (?, ?)',
      [userPhone, broadcastId]
    );
    connection.release();
    return true;
  } catch (error) {
    console.error('Ошибка markBroadcastRead:', error.message);
    return false;
  }
}

// Обновление статуса онлайн в БД
async function updateOnlineStatus(phone, isOnline) {
  if (!pool) return;
  
  try {
    const connection = await pool.getConnection();
    await connection.execute(
      'UPDATE users SET is_online = ?, last_active = NOW() WHERE phone = ?',
      [isOnline ? 1 : 0, phone]
    );
    connection.release();
  } catch (error) {
    console.error('Ошибка updateOnlineStatus:', error.message);
  }
}

// Сохранение личного сообщения в БД
async function savePrivateMessage(from, to, text) {
  if (!pool) return false;
  
  try {
    const connection = await pool.getConnection();
    const now = Math.floor(Date.now() / 1000);
    
    await connection.execute(
      'INSERT INTO messages (from_phone, to_phone, text, time, status) VALUES (?, ?, ?, ?, ?)',
      [from, to, text || '', now, 'sent']
    );
    
    connection.release();
    
    // Инвалидируем кэш чатов для обоих пользователей
    chatCache.delete(from);
    chatCache.delete(to);
    
    return true;
  } catch (error) {
    console.error('Ошибка savePrivateMessage:', error.message);
    return false;
  }
}

// Сохранение группового сообщения в БД
async function saveGroupMessage(groupId, from, text) {
  if (!pool) return false;
  
  try {
    const connection = await pool.getConnection();
    const now = Math.floor(Date.now() / 1000);
    
    await connection.execute(
      'INSERT INTO group_messages (group_id, from_phone, text, time, status) VALUES (?, ?, ?, ?, ?)',
      [groupId, from, text || '', now, 'sent']
    );
    
    // Инвалидируем кэш чатов для всех участников группы
    const [members] = await connection.execute(
      'SELECT user_phone FROM group_members WHERE group_id = ?',
      [groupId]
    );
    for (const member of members) {
      chatCache.delete(member.user_phone);
    }
    
    connection.release();
    return true;
  } catch (error) {
    console.error('Ошибка saveGroupMessage:', error.message);
    return false;
  }
}

// Отметить сообщения как прочитанные
async function markMessagesAsRead(myPhone, contactPhone) {
  if (!pool) return;
  
  try {
    const connection = await pool.getConnection();
    await connection.execute(
      'UPDATE messages SET status = "read", is_read = 1 WHERE from_phone = ? AND to_phone = ? AND (status != "read" OR is_read = 0)',
      [contactPhone, myPhone]
    );
    connection.release();
    
    // Инвалидируем кэш чатов
    chatCache.delete(myPhone);
    chatCache.delete(contactPhone);
  } catch (error) {
    console.error('Ошибка markMessagesAsRead:', error.message);
  }
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
  transports: ['websocket', 'polling'],
  connectionStateRecovery: {
    maxDisconnectionDuration: 2 * 60 * 1000,
    skipMiddlewares: true,
  }
});

// Middleware для логирования
io.use((socket, next) => {
  console.log(`🔌 Новое соединение: ${socket.id}`);
  next();
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
  
  // ========== РЕГИСТРАЦИЯ ПОЛЬЗОВАТЕЛЯ ==========
  socket.on('register', async (data) => {
    userPhone = data.phone;
    userName = data.name || data.phone;
    
    // Сохраняем в хранилища
    onlineUsers.set(userPhone, {
      socketId: socket.id,
      name: userName,
      lastSeen: Date.now()
    });
    userSockets.set(socket.id, userPhone);
    
    // Обновляем в БД
    await updateOnlineStatus(userPhone, true);
    
    console.log(`📝 Зарегистрирован: ${userName} (${userPhone})`);
    console.log(`👥 Онлайн: ${onlineUsers.size} пользователей`);
    
    // Рассылаем статус всем
    io.emit('user_status', {
      phone: userPhone,
      name: userName,
      is_online: true
    });
    
    // Отправляем текущие настройки темы
    socket.emit('theme_settings', {
      colors: currentThemeSettings.colors,
      background_url: currentThemeSettings.background_url
    });
    
    // Отправляем активную рассылку (если есть)
    const broadcast = await getActiveBroadcast(userPhone);
    if (broadcast) {
      socket.emit('broadcast_message', {
        id: broadcast.id,
        message: broadcast.message,
        from_phone: broadcast.from_phone
      });
    }
    
    // Отправляем список чатов
    const chats = await getChats(userPhone);
    socket.emit('chats_list', chats);
    
    // Восстанавливаем историю сообщений (если есть)
    const history = messageHistory.get(socket.id);
    if (history && history.length > 0) {
      socket.emit('message_history', history);
      messageHistory.delete(socket.id);
    }
  });
  
  // ========== ЗАПРОС СПИСКА ЧАТОВ ==========
  socket.on('get_chats', async (callback) => {
    if (!userPhone) return;
    const chats = await getChats(userPhone);
    if (callback && typeof callback === 'function') {
      callback(chats);
    } else {
      socket.emit('chats_list', chats);
    }
  });
  
  // ========== ПОЛУЧЕНИЕ СООБЩЕНИЙ (ЛИЧНЫЕ) ==========
  socket.on('get_messages', async (data) => {
    if (!userPhone || !data.contact_phone) return;
    
    try {
      const connection = await pool.getConnection();
      
      const [messages] = await connection.execute(`
        SELECT id, from_phone, text, file_path, file_name, file_type, file_size, 
               audio_path, audio_duration, time, status
        FROM messages 
        WHERE ((from_phone = ? AND to_phone = ?) OR (from_phone = ? AND to_phone = ?))
        AND deleted_at IS NULL
        ORDER BY time ASC
      `, [userPhone, data.contact_phone, data.contact_phone, userPhone]);
      
      connection.release();
      
      socket.emit('messages_list', { contact_phone: data.contact_phone, messages });
      
      // Отмечаем как прочитанные
      await markMessagesAsRead(userPhone, data.contact_phone);
      
    } catch (error) {
      console.error('Ошибка get_messages:', error.message);
      socket.emit('messages_error', { error: error.message });
    }
  });
  
  // ========== ПОЛУЧЕНИЕ СООБЩЕНИЙ ГРУППЫ ==========
socket.on('get_group_messages', async (data) => {
  if (!userPhone || !data.group_id) return;
  
  try {
    const connection = await pool.getConnection();
    
    const [messages] = await connection.execute(`
      SELECT id, from_phone, text, file_path, file_name, file_type, file_size,
             audio_path, audio_duration, time, status
      FROM group_messages
      WHERE group_id = ? AND deleted_at IS NULL
      ORDER BY time ASC
    `, [data.group_id]);
    
    // Получаем имена отправителей (connection ЕЩЁ открыт)
    for (const msg of messages) {
      const [user] = await connection.execute(
        'SELECT name FROM users WHERE phone = ?',
        [msg.from_phone]
      );
      msg.from_name = user.length > 0 ? user[0].name : msg.from_phone;
    }
    
    // Отмечаем как прочитанные (connection ВСЁ ЕЩЁ открыт)
    await connection.execute(
      'UPDATE group_messages SET is_read = 1 WHERE group_id = ? AND from_phone != ? AND is_read = 0',
      [data.group_id, userPhone]
    );
    
    connection.release(); // ✅ Теперь можно закрыть
    
    socket.emit('group_messages_list', { group_id: data.group_id, messages });
    
  } catch (error) {
    console.error('Ошибка get_group_messages:', error.message);
    socket.emit('messages_error', { error: error.message });
  }
});
  // ========== ОТПРАВКА ЛИЧНОГО СООБЩЕНИЯ ==========
  socket.on('send_private_message', async (data) => {
    if (!userPhone || !data.to) return;
    
    console.log(`📨 Личное сообщение от ${userPhone} к ${data.to}`);
    messageCount++;
    
    // Сохраняем в БД
    await savePrivateMessage(userPhone, data.to, data.text);
    
    // Сохраняем в историю для восстановления
    const msgData = {
      id: Date.now(),
      from: userPhone,
      to: data.to,
      text: data.text,
      type: 'private',
      time: Math.floor(Date.now() / 1000)
    };
    
    if (!messageHistory.has(socket.id)) {
      messageHistory.set(socket.id, []);
    }
    const history = messageHistory.get(socket.id);
    history.push(msgData);
    if (history.length > 50) history.shift();
    
    // Отправляем получателю (если онлайн)
    const target = onlineUsers.get(data.to);
    if (target) {
      io.to(target.socketId).emit('new_private_message', {
        id: msgData.id,
        from: userPhone,
        from_name: userName,
        text: data.text,
        time: msgData.time
      });
      
      // Обновляем чаты у получателя
      const targetChats = await getChats(data.to);
      io.to(target.socketId).emit('chats_list', targetChats);
    }
    
    // Обновляем чаты у отправителя
    const senderChats = await getChats(userPhone);
    socket.emit('chats_list', senderChats);
    
    socket.emit('message_sent', { success: true, id: msgData.id });
  });
  
  // ========== ОТПРАВКА ГРУППОВОГО СООБЩЕНИЯ ==========
  socket.on('send_group_message', async (data) => {
    if (!userPhone || !data.group_id) return;
    
    console.log(`📨 Групповое сообщение от ${userPhone} в группу ${data.group_id}`);
    messageCount++;
    
    // Сохраняем в БД
    await saveGroupMessage(data.group_id, userPhone, data.text);
    
    const msgData = {
      id: Date.now(),
      from: userPhone,
      from_name: userName,
      group_id: data.group_id,
      text: data.text,
      type: 'group',
      time: Math.floor(Date.now() / 1000)
    };
    
    // Рассылаем всем в группе
    socket.to(`group_${data.group_id}`).emit('new_group_message', msgData);
    socket.emit('message_sent', { success: true, id: msgData.id });
    
    // Обновляем чаты у всех участников группы
    try {
      const connection = await pool.getConnection();
      const [members] = await connection.execute(
        'SELECT user_phone FROM group_members WHERE group_id = ?',
        [data.group_id]
      );
      
      for (const member of members) {
        const memberSocket = onlineUsers.get(member.user_phone);
        if (memberSocket) {
          const memberChats = await getChats(member.user_phone);
          io.to(memberSocket.socketId).emit('chats_list', memberChats);
        }
        chatCache.delete(member.user_phone);
      }
      connection.release();
    } catch (error) {
      console.error('Ошибка обновления чатов группы:', error.message);
    }
  });
  
  // ========== СТАТУС ПЕЧАТИ (ЛИЧНЫЙ) ==========
  socket.on('typing_start', (data) => {
    if (!userPhone || !data.to) return;
    const target = onlineUsers.get(data.to);
    if (target) {
      io.to(target.socketId).emit('user_typing', {
        from: userPhone,
        from_name: userName,
        is_typing: true
      });
    }
  });
  
  socket.on('typing_stop', (data) => {
    if (!userPhone || !data.to) return;
    const target = onlineUsers.get(data.to);
    if (target) {
      io.to(target.socketId).emit('user_typing', {
        from: userPhone,
        from_name: userName,
        is_typing: false
      });
    }
  });
  
  // ========== СТАТУС ПЕЧАТИ (ГРУППОВОЙ) ==========
  socket.on('group_typing_start', (data) => {
    if (!userPhone || !data.group_id) return;
    socket.to(`group_${data.group_id}`).emit('group_typing', {
      from: userPhone,
      from_name: userName,
      group_id: data.group_id,
      is_typing: true
    });
  });
  
  socket.on('group_typing_stop', (data) => {
    if (!userPhone || !data.group_id) return;
    socket.to(`group_${data.group_id}`).emit('group_typing', {
      from: userPhone,
      from_name: userName,
      group_id: data.group_id,
      is_typing: false
    });
  });
  
  // ========== ОТМЕТКА О ПРОЧТЕНИИ ==========
  socket.on('mark_read', async (data) => {
    if (!userPhone || !data.contact_phone) return;
    await markMessagesAsRead(userPhone, data.contact_phone);
    
    // Уведомляем собеседника
    const target = onlineUsers.get(data.contact_phone);
    if (target) {
      io.to(target.socketId).emit('messages_read', {
        by: userPhone,
        chat_id: userPhone
      });
    }
    
    // Обновляем чаты
    const chats = await getChats(userPhone);
    socket.emit('chats_list', chats);
  });
  
  // ========== ОБНОВЛЕНИЕ СТАТУСА ==========
  socket.on('update_status', async (data) => {
    if (userPhone) {
      await updateOnlineStatus(userPhone, data.is_online);
      io.emit('user_status', {
        phone: userPhone,
        name: userName,
        is_online: data.is_online
      });
    }
  });
  
  // ========== ПОДТВЕРЖДЕНИЕ ПРОЧТЕНИЯ РАССЫЛКИ ==========
  socket.on('mark_broadcast_read', async (data) => {
    if (userPhone && data.broadcast_id) {
      await markBroadcastRead(userPhone, data.broadcast_id);
    }
  });
  
  // ========== АДМИН: ОБНОВЛЕНИЕ ТЕМЫ ==========
  socket.on('admin_update_theme', async (data, callback) => {
    // Проверяем, что отправитель — админ
    if (userPhone !== 'admin') {
      if (callback) callback({ success: false, error: 'Unauthorized' });
      return;
    }
    
    console.log('🎨 Админ обновил тему');
    
    // Обновляем настройки в памяти
    if (data.colors) {
      currentThemeSettings.colors = data.colors;
      currentThemeSettings.last_update = Date.now();
    }
    
    if (data.background_url) {
      currentThemeSettings.background_url = data.background_url;
    }
    
    // Рассылаем ВСЕМ пользователям
    io.emit('theme_changed', {
      colors: currentThemeSettings.colors,
      background_url: currentThemeSettings.background_url
    });
    
    if (callback) callback({ success: true });
  });
  
  // ========== ВХОД В ГРУППУ (JOIN) ==========
  socket.on('join_group', (data) => {
    if (!userPhone || !data.group_id) return;
    socket.join(`group_${data.group_id}`);
    console.log(`👥 ${userPhone} присоединился к группе ${data.group_id}`);
  });
  
  socket.on('leave_group', (data) => {
    if (!data.group_id) return;
    socket.leave(`group_${data.group_id}`);
    console.log(`👋 ${userPhone || '?'} покинул группу ${data.group_id}`);
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
      await updateOnlineStatus(phone, false);
      io.emit('user_status', { phone, is_online: false });
      console.log(`👤 ${phone} вышел, онлайн: ${onlineUsers.size}`);
    }
  });
});

// ========== ФОНОВЫЕ ЗАДАЧИ ==========

// Проверка неактивных пользователей (раз в 30 секунд)
setInterval(() => {
  const now = Date.now();
  for (const [phone, user] of onlineUsers.entries()) {
    if (now - user.lastSeen > INACTIVE_TIMEOUT) {
      console.log(`⏰ ${phone} неактивен, отмечаем оффлайн`);
      onlineUsers.delete(phone);
      updateOnlineStatus(phone, false);
      io.emit('user_status', { phone, is_online: false });
    }
  }
}, 30000);

// Статистика (раз в минуту)
setInterval(() => {
  const now = Date.now();
  const minutes = (now - lastStatsReset) / 1000 / 60;
  const messagesPerMinute = minutes > 0 ? Math.round(messageCount / minutes) : 0;
  
  const stats = {
    online: onlineUsers.size,
    messagesPerMinute: messagesPerMinute,
    memoryUsage: Math.round(process.memoryUsage().rss / 1024 / 1024),
    cacheSize: chatCache.size,
    uptime: Math.floor(process.uptime())
  };
  
  console.log('📊 Статистика:', stats);
  
  // Отправляем админу (если онлайн)
  const adminSocketId = onlineUsers.get('admin');
  if (adminSocketId) {
    io.to(adminSocketId).emit('stats_update', stats);
  }
  
  messageCount = 0;
  lastStatsReset = now;
}, 60000);

// Очистка кэша чатов (каждые 5 минут)
setInterval(() => {
  const now = Date.now();
  let deletedCount = 0;
  for (const [phone, data] of chatCache.entries()) {
    if (now - data.timestamp > CACHE_TTL * 2) {
      chatCache.delete(phone);
      deletedCount++;
    }
  }
  if (deletedCount > 0) {
    console.log(`🧹 Очищено ${deletedCount} записей кэша, осталось ${chatCache.size}`);
  }
}, 300000);

// Периодическая проверка темы
setInterval(async () => {
  try {
    const https = require('https');
    const options = {
      hostname: 'lexchat.rf.gd',
      path: '/colors.json',
      method: 'HEAD',
      timeout: 3000
    };
    
    const req = https.request(options, (res) => {
      if (res.statusCode === 200) {
        // Файл существует, загружаем если изменился
        loadThemeSettings();
      }
    });
    req.on('error', () => {});
    req.end();
  } catch(e) {}
}, 300000);

// ========== HTTP ENDPOINTS ==========

// Health check
app.get('/healthz', (req, res) => {
  res.json({ 
    status: 'ok', 
    connections: io.engine.clientsCount,
    dbConnected: pool !== null,
    online: onlineUsers.size,
    uptime: process.uptime()
  });
});

// Статистика
app.get('/stats', (req, res) => {
  res.json({
    online: onlineUsers.size,
    connections: io.engine.clientsCount,
    cache_size: chatCache.size,
    memory_usage_mb: Math.round(process.memoryUsage().rss / 1024 / 1024),
    uptime_seconds: Math.floor(process.uptime())
  });
});

// Получить текущую тему (для админки)
app.get('/theme', (req, res) => {
  res.json({
    colors: currentThemeSettings.colors,
    background_url: currentThemeSettings.background_url,
    last_update: currentThemeSettings.last_update
  });
});

// ========== ЗАПУСК СЕРВЕРА ==========
server.listen(PORT, () => {
  console.log(`\n🚀 WebSocket сервер запущен на порту ${PORT}`);
  console.log(`📡 Health check: https://lexchat-websocket.onrender.com/healthz`);
  console.log(`📊 Статистика: https://lexchat-websocket.onrender.com/stats`);
  console.log(`🎨 Тема: https://lexchat-websocket.onrender.com/theme`);
  console.log(`\n⚡ Готов к работе!\n`);
});
