// Инициализация с настройками переподключения [citation:9]
const socket = io('wss://your-app.onrender.com', {
  reconnection: true,
  reconnectionAttempts: Infinity,
  reconnectionDelay: 1000,
  reconnectionDelayMax: 10000,
  randomizationFactor: 0.5,
  timeout: 20000,
  transports: ['websocket'],
  autoConnect: true,
  // Для CORS с cookie [citation:5]
  withCredentials: true
});

// Отслеживание состояния восстановления [citation:8]
let missedMessages = [];

socket.on('connect', () => {
  console.log('🟢 Подключено к серверу');
  
  // Проверка восстановления сессии
  if (socket.recovered) {
    console.log('🔄 Сессия восстановлена');
    // Отправляем пропущенные сообщения
    if (missedMessages.length > 0) {
      missedMessages.forEach(msg => socket.emit('message', msg));
      missedMessages = [];
    }
  }
});

// Обработка отключения с автоматическим переподключением [citation:3]
socket.on('disconnect', (reason) => {
  console.log('🔴 Отключено:', reason);
  // Socket.IO автоматически переподключится
});

// Обработка ошибок подключения
socket.on('connect_error', (error) => {
  console.error('Ошибка подключения:', error.message);
});

// Получение сообщений
socket.on('message', (data) => {
  console.log('Новое сообщение:', data);
  // Ваша логика добавления сообщения в чат
});

// Отправка сообщений с буферизацией при оффлайн
function sendMessage(text) {
  const message = { text, timestamp: Date.now() };
  
  if (socket.connected) {
    socket.emit('message', message);
  } else {
    // Сохраняем для отправки после восстановления
    missedMessages.push(message);
    console.log('📦 Сохранено для отправки после подключения');
  }
}