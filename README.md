# 加杠杆交易系统

这是一个基于Python的自动化加杠杆交易系统，支持在加密货币交易所进行自动化交易。

## 功能特点

- 市场分析与趋势预测
- 动态仓位管理与杠杆调整
- 智能风险控制
- 自动化交易执行
- 完整的日志记录

## 系统要求

- Python 3.9+
- Windows 11/10

## 安装

1. 克隆仓库：
```bash
git clone [repository_url]
cd [repository_name]
```

2. 安装依赖：
```bash
pip install -r requirements.txt
```

3. 配置环境变量：
```bash
cp .env.example .env
```
然后编辑 `.env` 文件，填入你的API密钥和其他配置。

## 目录结构

```
├── src/                # 源代码
│   ├── market_analysis.py    # 市场分析模块
│   ├── position_manager.py   # 仓位管理模块
│   ├── risk_manager.py      # 风险控制模块
│   ├── exchange_api.py      # 交易所API接口
│   └── main.py             # 主程序入口
├── config/             # 配置文件
├── logs/              # 日志文件
├── tests/             # 测试文件
├── requirements.txt    # 项目依赖
├── .env.example       # 环境变量示例
└── README.md          # 项目说明
```

## 使用方法

1. 确保已正确配置 `.env` 文件
2. 运行主程序：
```bash
python src/main.py
```

## 风险提示

本系统涉及加杠杆交易，具有高风险性。请在使用前充分了解相关风险，并根据自己的风险承受能力谨慎使用。

## 许可证

MIT License 