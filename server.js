const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const https = require('https');
const http = require('http');

const app = express();
const server = createServer(app);
const PORT = process.env.PORT || 10000;

// Хранилище онлайн пользователей
const onlineUsers = new Map();

// Функция запроса к api.php на вашем хостинге
async function callApi(endpoint, params = {}) {
    return new Promise((resolve, reject) => {
        const url = new URL('https://lexchat.rf.gd/api.php');
        url.searchParams.append('action', endpoint);
        for (const [key, value] of Object.entries(params)) {
            url.searchParams.append(key, value);
        }
        
        const protocol = url.protocol === 'https:' ? https : http;
        const options = {
            hostname: url.hostname,
            path: url.pathname + url.search,
            method: 'GET',
            headers: {
                'User-Agent': 'LexChat-WebSocket/1.0'
            },
            timeout: 5000
        };
        
        const req = protocol.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    const json = JSON.parse(data);
                    resolve(json);
                } catch (e) {
                    reject(new Error('Invalid JSON response'));
                }
            });
        });
        
        req.on('error', (err) => reject(err));
        req.on('timeout', () => {
            req.destroy();
            reject(new Error('Request timeout'));
        });
        
        req.end();
    });
}

// Получение списка чатов из БД через api.php
async function getChatsFromDB(phone) {
    try {
        const result = await callApi('get_chats', { my_phone: phone });
        if (result && !result.error && Array.isArray(result)) {
            // Добавляем статус онлайн из WebSocket памяти
            return result.map(chat => {
                if (chat.type === 'user') {
                    chat.is_online = onlineUsers.has(chat.id);
                }
                return chat;
            });
        }
        return [];
    } catch (error) {
        console.error('Ошибка getChatsFromDB:', error.message);
        return [];
    }
}

const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] },
    pingTimeout: 60000,
    transports: ['websocket', 'polling']
});

io.on('connection', (socket) => {
    console.log('✅ Клиент подключен:', socket.id);
    let userPhone = null;
    let userName = null;

    // Регистрация пользователя
    socket.on('register', async (data) => {
        userPhone = data.phone;
        userName = data.name || data.phone;
        
        onlineUsers.set(userPhone, { socketId: socket.id, name: userName });
        console.log(`📝 Зарегистрирован: ${userName} (${userPhone})`);
        console.log(`👥 Онлайн: ${onlineUsers.size}`);
        
        // Рассылаем статус всем
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
            background_url: '/fonDefault.png'
        });
        
        // Отправляем список чатов из БД
        const chats = await getChatsFromDB(userPhone);
        console.log(`📋 Отправлено ${chats.length} чатов для ${userPhone}`);
        socket.emit('chats_list', chats);
    });
    
    // Запрос списка чатов
    socket.on('get_chats', async (callback) => {
        console.log(`📋 Запрос get_chats от ${userPhone}`);
        if (!userPhone) return;
        
        const chats = await getChatsFromDB(userPhone);
        console.log(`✅ Возвращаю ${chats.length} чатов`);
        
        if (callback && typeof callback === 'function') {
            callback(chats);
        } else {
            socket.emit('chats_list', chats);
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
