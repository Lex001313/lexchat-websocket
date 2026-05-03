const express = require('express');
const WebSocket = require('ws');
const cors = require('cors');

// ========== НАСТРОЙКИ ==========
const PORT = process.env.PORT || 3000;

// Хранилища
const onlineUsers = new Map();     // phone -> { ws, name, lastSeen, ip }
const userSessions = new Map();    // ws -> phone

// Статистика для отладки
const stats = {
    messagesReceived: 0,
    messagesSent: 0,
    commandsReceived: {},
    connections: 0
};

// ========== EXPRESS (для healthz и API) ==========
const app = express();
app.use(cors());
app.use(express.json());

// Health check для UptimeRobot / Render
app.get('/healthz', (req, res) => {
    res.json({
        status: 'ok',
        connections: onlineUsers.size,
        uptime: process.uptime(),
        timestamp: new Date().toISOString(),
        stats: {
            totalMessagesReceived: stats.messagesReceived,
            totalMessagesSent: stats.messagesSent,
            commands: stats.commandsReceived
        }
    });
});

// Получить список онлайн пользователей (для отладки)
app.get('/debug/online', (req, res) => {
    const users = Array.from(onlineUsers.entries()).map(([phone, data]) => ({
        phone,
        name: data.name,
        lastSeen: new Date(data.lastSeen).toISOString(),
        ip: data.ip
    }));
    res.json({ 
        count: users.length, 
        users,
        uptime: process.uptime()
    });
});

// Принудительно отправить команду пользователю (для отладки)
app.post('/debug/send', (req, res) => {
    const { phone, type, data } = req.body;
    const sent = sendToUser(phone, type, data);
    res.json({ success: sent, phone, type });
});

// ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
function log(type, message, data = null) {
    const timestamp = new Date().toISOString();
    const coloredType = {
        'CONNECT': '\x1b[32m',    // зелёный
        'DISCONNECT': '\x1b[31m', // красный
        'MESSAGE': '\x1b[36m',    // голубой
        'SEND': '\x1b[35m',       // фиолетовый
        'BROADCAST': '\x1b[33m',  // жёлтый
        'ERROR': '\x1b[41m',      // красный фон
        'REGISTER': '\x1b[42m',   // зелёный фон
        'DEBUG': '\x1b[90m'       // серый
    }[type] || '\x1b[0m';
    
    console.log(`${coloredType}[${timestamp}] [${type}] ${message}\x1b[0m`);
    if (data) {
        console.log(`  └─ ${JSON.stringify(data, null, 2).replace(/\n/g, '\n     ')}`);
    }
}

function broadcastToAll(type, data, excludePhone = null) {
    let sent = 0;
    onlineUsers.forEach((client, phone) => {
        if (excludePhone !== phone && client.ws.readyState === WebSocket.OPEN) {
            client.ws.send(JSON.stringify({ type, ...data }));
            sent++;
        }
    });
    log('BROADCAST', `${type} -> ${sent} clients`, data);
    stats.messagesSent += sent;
    return sent;
}

function sendToUser(phone, type, data) {
    const client = onlineUsers.get(phone);
    if (client && client.ws.readyState === WebSocket.OPEN) {
        client.ws.send(JSON.stringify({ type, ...data }));
        log('SEND', `${type} -> ${phone}`, data);
        stats.messagesSent++;
        return true;
    }
    log('DEBUG', `${type} -> ${phone} (offline, not sent)`, data);
    return false;
}

function broadcastToGroup(groupId, type, data, excludePhone = null) {
    // Отправка всем участникам группы (группы хранятся в кэше сервера)
    // Пока просто заглушка, в полной версии нужно загружать участников из БД
    log('DEBUG', `Group broadcast ${groupId} -> ${type} (requires DB integration)`, data);
    // Для прототипа: рассылаем всем онлайн
    return broadcastToAll(type, { group_id: groupId, ...data }, excludePhone);
}

// ========== WEBSOCKET СЕРВЕР ==========
const wss = new WebSocket.Server({ port: PORT });

wss.on('connection', (ws, req) => {
    const clientIp = req.socket.remoteAddress;
    let userPhone = null;
    let heartbeatInterval = null;
    let reconnectAttempts = 0;
    
    stats.connections++;
    log('CONNECT', `New client from ${clientIp} (total: ${onlineUsers.size})`);
    
    // Heartbeat для поддержания соединения (каждые 30 секунд)
    heartbeatInterval = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'ping', timestamp: Date.now() }));
        }
    }, 30000);
    
    ws.on('message', async (rawData) => {
        try {
            const data = JSON.parse(rawData);
            stats.messagesReceived++;
            
            // Считаем статистику по командам
            const cmd = data.type;
            stats.commandsReceived[cmd] = (stats.commandsReceived[cmd] || 0) + 1;
            
            log('MESSAGE', `From ${userPhone || 'unregistered'}`, data);
            
            switch (data.type) {
                // ========== РЕГИСТРАЦИЯ ПОЛЬЗОВАТЕЛЯ ==========
                case 'register':
                    userPhone = data.phone;
                    const existing = onlineUsers.get(userPhone);
                    if (existing) {
                        log('REGISTER', `Replacing existing connection for ${userPhone}`);
                        if (existing.ws.readyState === WebSocket.OPEN) {
                            existing.ws.close();
                        }
                    }
                    
                    onlineUsers.set(userPhone, {
                        ws: ws,
                        name: data.name || userPhone,
                        lastSeen: Date.now(),
                        ip: clientIp
                    });
                    userSessions.set(ws, userPhone);
                    
                    log('REGISTER', `✅ ${userPhone} (${data.name || userPhone}) online. Total: ${onlineUsers.size}`);
                    
                    // Оповещаем всех об изменении статуса
                    broadcastToAll('user_status', {
                        phone: userPhone,
                        name: data.name || userPhone,
                        is_online: true
                    }, userPhone);
                    
                    // Отправляем подтверждение
                    ws.send(JSON.stringify({
                        type: 'registered',
                        success: true,
                        phone: userPhone,
                        timestamp: Date.now()
                    }));
                    break;
                
                // ========== НОВОЕ СООБЩЕНИЕ (пересылка получателю) ==========
                case 'new_message':
                    if (!userPhone) {
                        ws.send(JSON.stringify({ type: 'error', message: 'Not registered' }));
                        break;
                    }
                    
                    if (data.to) {
                        // Личное сообщение
                        sendToUser(data.to, 'new_message', {
                            subtype: data.subtype || 'text',
                            from: userPhone,
                            to: data.to,
                            msg_id: data.msg_id,
                            time: data.time || Math.floor(Date.now() / 1000),
                            data: data.data || {}
                        });
                    } else if (data.group_id) {
                        // Групповое сообщение
                        broadcastToGroup(data.group_id, 'new_message', {
                            subtype: data.subtype || 'text',
                            group_id: data.group_id,
                            from: userPhone,
                            msg_id: data.msg_id,
                            time: data.time || Math.floor(Date.now() / 1000),
                            data: data.data || {}
                        }, userPhone);
                    }
                    
                    // Подтверждение отправителю
                    ws.send(JSON.stringify({
                        type: 'message_sent',
                        msg_id: data.msg_id,
                        time: data.time || Math.floor(Date.now() / 1000)
                    }));
                    break;
                
                // ========== УДАЛЕНИЕ СООБЩЕНИЯ ==========
                case 'delete_message':
                    if (!userPhone) break;
                    
                    if (data.to) {
                        sendToUser(data.to, 'delete_message', {
                            msg_id: data.msg_id,
                            chat_type: 'private',
                            from: userPhone
                        });
                    } else if (data.group_id) {
                        broadcastToGroup(data.group_id, 'delete_message', {
                            msg_id: data.msg_id,
                            chat_type: 'group',
                            group_id: data.group_id,
                            from: userPhone
                        }, userPhone);
                    }
                    
                    ws.send(JSON.stringify({
                        type: 'message_deleted',
                        msg_id: data.msg_id,
                        success: true
                    }));
                    break;
                
                // ========== ИНДИКАТОР ПЕЧАТАЕТ ==========
                case 'typing':
                    if (!userPhone) break;
                    
                    if (data.to) {
                        sendToUser(data.to, 'typing', {
                            from: userPhone,
                            is_typing: data.is_typing !== false
                        });
                    } else if (data.group_id) {
                        broadcastToGroup(data.group_id, 'typing', {
                            from: userPhone,
                            group_id: data.group_id,
                            is_typing: data.is_typing !== false
                        }, userPhone);
                    }
                    break;
                
                // ========== ОТМЕТКА О ПРОЧТЕНИИ ==========
                case 'mark_read':
                    if (!userPhone) break;
                    
                    if (data.from) {
                        sendToUser(data.from, 'mark_read', {
                            by: userPhone,
                            chat_type: 'private',
                            msg_ids: data.msg_ids || []
                        });
                    } else if (data.group_id) {
                        broadcastToGroup(data.group_id, 'mark_read', {
                            by: userPhone,
                            group_id: data.group_id,
                            msg_ids: data.msg_ids || []
                        }, userPhone);
                    }
                    break;
                
                // ========== ЗАПРОС СТАТУСА ПОЛЬЗОВАТЕЛЯ ==========
                case 'get_status':
                    if (!userPhone) break;
                    
                    const targetUser = onlineUsers.get(data.phone);
                    ws.send(JSON.stringify({
                        type: 'user_status',
                        phone: data.phone,
                        is_online: !!targetUser,
                        name: targetUser ? targetUser.name : null
                    }));
                    break;
                
                // ========== НОВАЯ РАССЫЛКА ОТ АДМИНА ==========
                case 'new_broadcast':
                    if (!userPhone) break;
                    // Рассылаем всем (админ может отправлять)
                    broadcastToAll('new_broadcast', {
                        broadcast_id: data.broadcast_id,
                        message: data.message,
                        from_phone: data.from_phone || 'Администратор'
                    });
                    break;
                
                // ========== НОВЫЕ НАСТРОЙКИ (админ изменил) ==========
                case 'new_polling_settings':
                    if (!userPhone) break;
                    broadcastToAll('new_polling_settings', {
                        chats_poll_interval: data.chats_poll_interval,
                        broadcast_poll_interval: data.broadcast_poll_interval,
                        messages_poll_interval_fallback: data.messages_poll_interval_fallback,
                        disable_groups: data.disable_groups
                    });
                    break;
                
                // ========== НОВЫЕ ЦВЕТА (админ изменил) ==========
                case 'new_colors':
                    if (!userPhone) break;
                    broadcastToAll('new_colors', {
                        colors: data.colors
                    });
                    break;
                
                // ========== НОВАЯ АВАТАРКА ==========
                case 'new_avatar':
                    if (!userPhone) break;
                    
                    if (data.to) {
                        sendToUser(data.to, 'new_avatar', {
                            phone: userPhone,
                            avatar: data.avatar
                        });
                    }
                    // Также обновляем в чатах
                    broadcastToAll('new_avatar', {
                        phone: userPhone,
                        avatar: data.avatar
                    }, userPhone);
                    break;
                
                // ========== ПЕРЕКЛЮЧЕНИЕ ГРУПП (админ) ==========
                case 'groups_toggle':
                    if (!userPhone) break;
                    broadcastToAll('groups_toggle', {
                        enabled: data.enabled
                    });
                    break;
                
                // ========== ОБНОВЛЕНИЕ СПИСКА ЧАТОВ ==========
                case 'new_chats':
                    if (!userPhone) break;
                    
                    if (data.to) {
                        sendToUser(data.to, 'new_chats', {
                            reason: data.reason || 'update',
                            from: userPhone
                        });
                    } else {
                        broadcastToAll('new_chats', {
                            reason: data.reason || 'update',
                            from: userPhone
                        }, userPhone);
                    }
                    break;
                
                // ========== ПРОВЕРКА СОЕДИНЕНИЯ (pong) ==========
                case 'pong':
                    // Просто обновляем время последней активности
                    if (userPhone) {
                        const user = onlineUsers.get(userPhone);
                        if (user) user.lastSeen = Date.now();
                    }
                    break;
                
                default:
                    log('WARNING', `Unknown message type: ${data.type}`);
                    ws.send(JSON.stringify({ 
                        type: 'error', 
                        message: 'Unknown command',
                        received: data.type 
                    }));
            }
        } catch (err) {
            log('ERROR', `Message parsing error: ${err.message}`);
            ws.send(JSON.stringify({ type: 'error', message: 'Invalid message format' }));
        }
    });
    
    ws.on('close', () => {
        clearInterval(heartbeatInterval);
        
        if (userPhone) {
            log('DISCONNECT', `${userPhone} disconnected`);
            onlineUsers.delete(userPhone);
            userSessions.delete(ws);
            
            // Оповещаем всех об уходе
            broadcastToAll('user_status', {
                phone: userPhone,
                is_online: false
            });
        }
        log('CONNECT', `Remaining connections: ${onlineUsers.size}`);
    });
    
    ws.on('error', (err) => {
        log('ERROR', `WebSocket error: ${err.message}`);
    });
});

// ========== ЗАПУСК СЕРВЕРА ==========
const server = app.listen(PORT, () => {
    console.log(`
╔═══════════════════════════════════════════════════════════════════════════════╗
║                    🚀 LexChat WebSocket Server v2.0                           ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║  HTTP Server:  http://localhost:${PORT}                                         ║
║  WS Server:    ws://localhost:${PORT}                                           ║
║  Health check: http://localhost:${PORT}/healthz                                 ║
║  Debug online: http://localhost:${PORT}/debug/online                            ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║  Команды WS:                                                                  ║
║  ├─ register, new_message, delete_message, typing, mark_read                  ║
║  ├─ get_status, new_broadcast, new_polling_settings, new_colors               ║
║  ├─ new_avatar, groups_toggle, new_chats, pong                                ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║  Статус:         ✅ Сервер запущен                                             ║
║  WebSocket порт: ${PORT}                                                         ║
║  Онлайн:         0                                                             ║
╚═══════════════════════════════════════════════════════════════════════════════╝
    `);
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('\n📡 SIGTERM received, closing server...');
    
    // Закрываем все WebSocket соединения
    onlineUsers.forEach((client, phone) => {
        if (client.ws.readyState === WebSocket.OPEN) {
            client.ws.close();
        }
    });
    
    wss.close(() => {
        console.log('✅ WebSocket server closed');
        server.close(() => {
            console.log('✅ HTTP server closed');
            process.exit(0);
        });
    });
});

process.on('SIGINT', () => {
    console.log('\n📡 SIGINT received, closing server...');
    process.exit(0);
});

// Вывод статистики каждые 5 минут (для отладки на Render)
setInterval(() => {
    if (onlineUsers.size > 0 || stats.messagesReceived > 0) {
        console.log(`
╔═══════════════════════════════════════════════════════════════════════════════╗
║ 📊 СТАТИСТИКА ЗА 5 МИНУТ                                                     ║
╠═══════════════════════════════════════════════════════════════════════════════╣
║  Онлайн:        ${onlineUsers.size}                                                    
║  Сообщений получено: ${stats.messagesReceived}                                                 
║  Сообщений отправлено: ${stats.messagesSent}                                                 
║  Команды:       ${Object.keys(stats.commandsReceived).length} типов                              
╚═══════════════════════════════════════════════════════════════════════════════╝
        `);
    }
}, 300000); // 5 минут
