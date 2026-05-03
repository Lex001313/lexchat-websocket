const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');

const app = express();
const server = createServer(app);
const PORT = process.env.PORT || 10000;

const onlineUsers = new Map();

const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] },
    pingTimeout: 60000,
    transports: ['websocket', 'polling']
});

// ТЕСТОВЫЕ ЧАТЫ (с правильными путями к аватаркам)
const testChats = [
    {
        id: '79069119232',
        type: 'user',
        name: 'Roman',
        avatar: 'https://lexchat.rf.gd/uploads/avatars/default.png',
        last_message: 'Привет! Как дела?',
        last_time: Math.floor(Date.now() / 1000),
        unread: false,
        is_online: true
    },
    {
        id: '79135711627',
        type: 'user',
        name: 'Валентина',
        avatar: 'https://lexchat.rf.gd/uploads/avatars/default.png',
        last_message: 'Добрый вечер!',
        last_time: Math.floor(Date.now() / 1000) - 3600,
        unread: false,
        is_online: true
    },
    {
        id: '777',
        type: 'group',
        name: '👥 Общий чат',
        avatar: 'https://lexchat.rf.gd/uploads/group_avatars/default.png',
        last_message: 'Всем привет!',
        last_time: Math.floor(Date.now() / 1000) - 7200,
        unread: false,
        is_online: false
    }
];

io.on('connection', (socket) => {
    console.log('✅ Клиент подключен:', socket.id);
    let userPhone = null;
    let userName = null;

    socket.on('register', (data) => {
        userPhone = data.phone;
        userName = data.name || data.phone;
        
        onlineUsers.set(userPhone, { socketId: socket.id, name: userName });
        console.log(`📝 Зарегистрирован: ${userName} (${userPhone})`);
        console.log(`👥 Онлайн: ${onlineUsers.size}`);
        
        io.emit('user_status', { phone: userPhone, name: userName, is_online: true });
        
        // Отправляем тему
        socket.emit('theme_settings', {
            colors: {
                dark_bg: '#0a0f12', dark_sidebar_bg: '#111b21', dark_header_bg: '#202c33',
                dark_text: '#e9edef', dark_message_in_bg: '#202c33', dark_message_out_bg: '#005c4b',
                dark_input_bg: '#2a3942', light_bg: '#ffffff', light_sidebar_bg: '#ffffff',
                light_header_bg: '#e9edef', light_text: '#111b21', light_message_in_bg: '#ffffff',
                light_message_out_bg: '#d9fdd3', light_input_bg: '#ffffff'
            },
            background_url: 'https://lexchat.rf.gd/fonDefault.png'
        });
        
        // Отправляем чаты
        socket.emit('chats_list', testChats);
    });
    
    // Запрос чатов
    socket.on('get_chats', (callback) => {
        console.log(`📋 Запрос get_chats от ${userPhone}`);
        if (callback && typeof callback === 'function') {
            callback(testChats);
        } else {
            socket.emit('chats_list', testChats);
        }
    });
    
    // Статус печати
    socket.on('typing_start', (data) => {
        const target = onlineUsers.get(data.to);
        if (target) {
            io.to(target.socketId).emit('user_typing', { from: data.from, from_name: userName, is_typing: true });
        }
    });
    
    socket.on('typing_stop', (data) => {
        const target = onlineUsers.get(data.to);
        if (target) {
            io.to(target.socketId).emit('user_typing', { from: data.from, from_name: userName, is_typing: false });
        }
    });
    
    // Обновление статуса
    socket.on('update_status', (data) => {
        console.log(`🟢 ${userPhone} ${data.is_online ? 'онлайн' : 'оффлайн'}`);
        io.emit('user_status', { phone: userPhone, name: userName, is_online: data.is_online });
    });
    
    socket.on('disconnect', () => {
        console.log('❌ Клиент отключен:', socket.id);
        if (userPhone) {
            onlineUsers.delete(userPhone);
            io.emit('user_status', { phone: userPhone, name: userName, is_online: false });
        }
    });
});

// Health check для админки
app.get('/healthz', (req, res) => {
    res.json({ 
        status: 'ok', 
        connections: io.engine.clientsCount,
        online: onlineUsers.size
    });
});

// Статистика
app.get('/stats', (req, res) => {
    res.json({
        online: onlineUsers.size,
        connections: io.engine.clientsCount
    });
});

server.listen(PORT, () => {
    console.log(`🚀 WebSocket сервер запущен на порту ${PORT}`);
    console.log(`📡 Health check: /healthz`);
});
