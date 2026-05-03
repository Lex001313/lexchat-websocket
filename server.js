const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');

const app = express();
const server = createServer(app);
const PORT = process.env.PORT || 10000;

// Хранилище онлайн пользователей
const onlineUsers = new Map();

const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] },
    pingTimeout: 60000,
    transports: ['websocket', 'polling']
});

io.on('connection', (socket) => {
    console.log('✅ Клиент подключен:', socket.id);
    let userPhone = null;
    let userName = null;

    // Регистрация
    socket.on('register', (data) => {
        userPhone = data.phone;
        userName = data.name || data.phone;
        
        onlineUsers.set(userPhone, { socketId: socket.id, name: userName });
        console.log(`📝 Зарегистрирован: ${userName} (${userPhone})`);
        console.log(`👥 Онлайн: ${onlineUsers.size}`);
        
        // Рассылаем статус всем
        io.emit('user_status', { phone: userPhone, name: userName, is_online: true });
        
        // Отправляем текущую тему
        socket.emit('theme_settings', {
            colors: {
                dark_bg: '#0a0f12', dark_sidebar_bg: '#111b21', dark_header_bg: '#202c33',
                dark_text: '#e9edef', dark_message_in_bg: '#202c33', dark_message_out_bg: '#005c4b',
                dark_input_bg: '#2a3942', light_bg: '#ffffff', light_sidebar_bg: '#ffffff',
                light_header_bg: '#e9edef', light_text: '#111b21', light_message_in_bg: '#ffffff',
                light_message_out_bg: '#d9fdd3', light_input_bg: '#ffffff'
            },
            background_url: '/fonDefault.png'
        });
    });
    
    // ЗАПРОС СПИСКА ЧАТОВ — ВОЗВРАЩАЕМ ТЕСТОВЫЕ ДАННЫЕ
    socket.on('get_chats', (callback) => {
        console.log(`📋 Запрос get_chats от ${userPhone}`);
        // ВРЕМЕННО: возвращаем тестовые чаты
        const testChats = [
            {
                id: '79069119232',
                type: 'user',
                name: 'Roman',
                avatar: 'uploads/avatars/default.png',
                last_message: 'Привет!',
                last_time: Math.floor(Date.now() / 1000),
                unread: false,
                is_online: true
            },
            {
                id: '79135711627',
                type: 'user',
                name: 'Валентина',
                avatar: 'uploads/avatars/default.png',
                last_message: 'Как дела?',
                last_time: Math.floor(Date.now() / 1000) - 3600,
                unread: false,
                is_online: true
            }
        ];
        
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
    
    // Отключение
    socket.on('disconnect', () => {
        console.log('❌ Клиент отключен:', socket.id);
        if (userPhone) {
            onlineUsers.delete(userPhone);
            io.emit('user_status', { phone: userPhone, name: userName, is_online: false });
        }
    });
});

// Health check
app.get('/healthz', (req, res) => {
    res.json({ status: 'ok', connections: io.engine.clientsCount, online: onlineUsers.size });
});

server.listen(PORT, () => {
    console.log(`🚀 WebSocket сервер запущен на порту ${PORT}`);
});
