const { createApp } = Vue

const app = createApp({
    data() {
        return {
            // 账户相关
            accounts: [
                { id: 'simulation', name: '模拟账户' },
                { id: 'live', name: '实盘账户' }
            ],
            selectedAccount: 'simulation',
            accountBalance: 0.00,

            // 交易状态
            isTrading: false,
            tradingStatus: '未启动',

            // 市场数据
            marketData: {
                headers: [
                    '交易对', 
                    '最新价格', 
                    '1分钟涨跌', 
                    '1分钟成交量', 
                    '成交笔数',
                    '更新次数',
                    '最高价',
                    '最低价',
                    'USDT成交量',
                    'Tick数',
                    '趋势强度',
                    '波动率',
                    'RSI',
                    '布林带位置',
                    '更新时间'
                ],
                data: [],
                lastUpdate: null
            },

            // 日志记录
            logs: [],

            // 持仓信息
            positions: [],
            
            // 盈利信息
            totalPnl: 0.00,
            pnlAmount: 0.00,

            // 历史记录
            history: [],

            // WebSocket连接
            ws: null,

            // 调试模式
            debug: true
        }
    },

    computed: {
        sortedMarketData() {
            return [...this.marketData.data].sort((a, b) => {
                // BTC始终第一
                if (a.symbol === 'BTCUSDT') return -1;
                if (b.symbol === 'BTCUSDT') return 1;
                // ETH始终第二
                if (a.symbol === 'ETHUSDT') return -1;
                if (b.symbol === 'ETHUSDT') return 1;
                // 其他按分数排序
                return (parseFloat(b.score) || 0) - (parseFloat(a.score) || 0);
            });
        }
    },

    created() {
        console.log('Vue应用已创建，准备初始化WebSocket连接');
        this.initWebSocket();
    },

    methods: {
        // 调试日志
        debugLog(message) {
            if (this.debug) {
                console.log(`[Debug] ${new Date().toLocaleTimeString()}: ${message}`);
            }
        },

        // 切换账户
        async switchAccount() {  // 移除accountId参数，使用data中的selectedAccount
            try {
                this.debugLog(`切换账户: ${this.selectedAccount}`)
                await axios.post('/api/switch_account', {
                    account_id: this.selectedAccount
                })
                await this.updateBalance()
                await this.updatePositions()
                this.addLog('success', `切换到${this.selectedAccount === 'simulation' ? '模拟账户' : '实盘账户'}`)
            } catch (error) {
                console.error('切换账户失败:', error)
                this.addLog('error', `切换账户失败: ${error.response?.data?.detail || error.message}`)
            }
        },

        // 启动交易
        async startTrading() {
            try {
                this.debugLog(`${this.isTrading ? '停止' : '启动'}交易`)
                if (this.isTrading) {
                    await axios.post('/api/stop_trading')
                    this.tradingStatus = '已停止'
                } else {
                    await axios.post('/api/start_trading')
                    this.tradingStatus = '交易中'
                }
                this.isTrading = !this.isTrading
                this.addLog('success', this.isTrading ? '启动自动交易' : '停止自动交易')
            } catch (error) {
                console.error('交易控制失败:', error)
                this.addLog('error', `${this.isTrading ? '停止' : '启动'}交易失败: ${error.response?.data?.detail || error.message}`)
            }
        },

        // 更新账户余额
        async updateBalance() {
            try {
                const response = await axios.get('/api/balance')
                this.debugLog(`更新余额: ${JSON.stringify(response.data)}`)
                this.accountBalance = Number(response.data.balance) || 0.00
                this.availableBalance = Number(response.data.available) || 0.00
                this.marginUsed = Number(response.data.margin) || 0.00
                this.positionValue = Number(response.data.position_value) || 0.00
            } catch (error) {
                console.error('获取余额失败:', error)
                this.addLog('error', `获取余额失败: ${error.response?.data?.detail || error.message}`)
            }
        },

        // 更新持仓信息
        async updatePositions() {
            try {
                const response = await axios.get('/api/positions')
                this.debugLog(`更新持仓: ${JSON.stringify(response.data)}`)
                this.positions = response.data.positions || []
                this.totalPnl = Number(response.data.totalPnl) || 0.00
                this.pnlAmount = Number(response.data.pnlAmount) || 0.00
            } catch (error) {
                console.error('更新持仓信息失败:', error)
                this.addLog('error', `更新持仓信息失败: ${error.response?.data?.detail || error.message}`)
            }
        },

        // 更新历史记录
        async updateHistory() {
            try {
                const response = await axios.get('/api/history')
                this.debugLog(`更新历史记录: ${JSON.stringify(response.data)}`)
                this.history = response.data || []
            } catch (error) {
                console.error('获取历史记录失败:', error)
                this.addLog('error', `获取历史记录失败: ${error.response?.data?.detail || error.message}`)
            }
        },

        // 更新市场数据
        updateMarketData(newData) {
            console.log('开始更新市场数据:', newData);
            try {
                if (!newData || !Array.isArray(newData.data)) {
                    console.error('无效的市场数据格式:', newData);
                    return;
                }
                
                this.marketData.lastUpdate = new Date(newData.timestamp).toLocaleTimeString();
                console.log('更新时间:', this.marketData.lastUpdate);
                
                this.marketData.data = newData.data.map(item => {
                    try {
                        return {
                            symbol: item.symbol || '',
                            price: Number(item.price) || 0,
                            priceChange: ((Number(item.price_change) || 0) * 100).toFixed(2) + '%',
                            volume: Number(item.volume) || 0,
                            trades: Number(item.trades) || 0,
                            updates: Number(item.updates) || 0,
                            high: Number(item.high) || 0,
                            low: Number(item.low) || 0,
                            quoteVolume: Number(item.quote_volume) || 0,
                            tickCount: Number(item.tick_count) || 0,
                            trendStrength: ((Number(item.trend_strength) || 0) * 100).toFixed(1) + '%',
                            volatility: ((Number(item.volatility) || 0) * 100).toFixed(2) + '%',
                            rsi: item.rsi ? item.rsi.toFixed(1) : '-',
                            score: item.score ? item.score.toFixed(1) : '-',
                            timestamp: new Date(item.timestamp).toLocaleTimeString()
                        };
                    } catch (err) {
                        console.error('处理市场数据项失败:', err, item);
                        return null;
                    }
                }).filter(item => item !== null);
                
                console.log('市场数据更新完成，共', this.marketData.data.length, '条记录');
            } catch (error) {
                console.error('更新市场数据失败:', error);
                this.debugLog(`更新市场数据失败: ${error.message}`);
            }
        },

        // 添加日志
        addLog(type, message) {
            const log = {
                type,
                message,
                timestamp: new Date().toLocaleTimeString()
            }
            this.logs.unshift(log)
            // 只保留最近的100条日志
            if (this.logs.length > 100) {
                this.logs.pop()
            }
            this.debugLog(`添加日志: ${type} - ${message}`)
        },

        // 格式化数字
        formatNumber(value) {
            if (typeof value === 'number') {
                if (value > 1000000) {
                    return (value / 1000000).toFixed(2) + 'M'
                } else if (value > 1000) {
                    return (value / 1000).toFixed(2) + 'K'
                }
                return value.toFixed(8)
            }
            return value
        },

        // 初始化WebSocket连接
        initWebSocket() {
            console.log('开始初始化WebSocket连接...');
            if (this.ws) {
                console.log('关闭现有WebSocket连接');
                this.ws.close();
            }

            this.debugLog('初始化WebSocket连接');
            this.ws = new WebSocket('ws://localhost:8000/ws');
            
            this.ws.onopen = () => {
                console.log('WebSocket连接已建立');
                this.debugLog('WebSocket连接已建立');
                this.addLog('success', 'WebSocket连接已建立');
            };
            
            this.ws.onclose = () => {
                console.log('WebSocket连接已断开，5秒后重试...');
                this.debugLog('WebSocket连接已断开，5秒后重试...');
                this.addLog('warning', 'WebSocket连接已断开，正在重试...');
                setTimeout(() => {
                    this.initWebSocket();
                }, 5000);
            };
            
            this.ws.onerror = (error) => {
                console.error('WebSocket错误:', error);
                this.debugLog(`WebSocket错误: ${error.message}`);
                this.addLog('error', `WebSocket错误: ${error.message}`);
            };
            
            this.ws.onmessage = (event) => {
                console.log('收到WebSocket消息:', event.data);
                try {
                    const data = JSON.parse(event.data);
                    console.log('解析后的数据:', data);
                    
                    switch (data.type) {
                        case 'market_data':
                            console.log('收到市场数据:', data.data);
                            this.updateMarketData(data.data);
                            break;
                        case 'log':
                            console.log('收到日志消息:', data.data);
                            this.addLog(data.data.type, data.data.message);
                            break;
                        case 'position_update':
                            console.log('收到持仓更新:', data.data);
                            this.positions = data.data.positions || [];
                            this.totalPnl = Number(data.data.totalPnl) || 0.00;
                            this.pnlAmount = Number(data.data.pnlAmount) || 0.00;
                            break;
                        case 'balance_update':
                            console.log('收到余额更新:', data.data);
                            this.accountBalance = Number(data.data.balance) || 0.00;
                            this.availableBalance = Number(data.data.available) || 0.00;
                            this.marginUsed = Number(data.data.margin) || 0.00;
                            this.positionValue = Number(data.data.position_value) || 0.00;
                            break;
                        case 'trade_history':
                            console.log('收到交易历史更新');
                            this.updateHistory();
                            break;
                        case 'status_update':
                            console.log('收到状态更新:', data.data);
                            this.tradingStatus = data.data.status || '未知状态';
                            break;
                        default:
                            console.log('未知消息类型:', data.type);
                    }
                } catch (error) {
                    console.error('处理WebSocket消息失败:', error, '原始数据:', event.data);
                    this.debugLog(`处理WebSocket消息失败: ${error.message}`);
                }
            };
        },

        async initializeData() {
            try {
                // 获取初始数据
                await this.updateBalance()
                await this.updatePositions()
                await this.updateHistory()
                
                this.debugLog('初��数据加载完成')
            } catch (error) {
                console.error('初始化数据失败:', error)
                this.addLog('error', `初始化数据失败: ${error.message}`)
            }
        },
    }
}).mount('#app'); 