# Rosetta数据下载与拆帧工具 - Streamlit版本

这是一个基于Web界面的数据下载和帧提取工具，支持从Rosetta平台下载项目数据并进行帧提取。

## 功能特点

- 🎨 **友好的Web界面**：无需命令行操作，通过网页表单输入参数
- 📊 **实时进度显示**：处理过程中显示进度条和状态信息
- 📁 **一键下载**：处理完成后可直接下载结果文件
- ⚙️ **灵活配置**：支持测试模式、拆帧开关等多种选项
- 🔒 **安全处理**：临时文件自动清理，保护用户数据

## 安装和运行

### 1. 安装依赖

```bash
cd /Users/Apple/task/integrate/frame_extraction_streamlit
pip install -r requirements.txt
```

### 2. 运行应用

```bash
streamlit run app.py
```

### 3. 访问应用

应用启动后，会自动打开浏览器访问 `http://localhost:8501`

## 使用说明

### 基本步骤

1. **输入项目信息**
   - 项目ID：输入要处理的项目ID
   - 项目名称：为项目设置一个显示名称
   - 池子ID：每行输入一个池子ID

2. **填写账号信息**
   - 用户名：Rosetta平台用户名
   - 密码：Rosetta平台密码

3. **选择处理选项**（侧边栏）
   - **池子类型**：选择"完成池"或"抽查池"
   - **测试模式**：跳过数据下载，使用已有数据
   - **启用拆帧**：是否执行帧提取操作

4. **开始处理**
   - 点击"开始处理"按钮
   - 等待处理完成

5. **下载结果**
   - 处理完成后点击下载按钮
   - 获取结果压缩包

### 参数说明

| 参数 | 说明 | 示例 |
|------|------|------|
| 项目ID | Rosetta平台中的项目标识 | 3603 |
| 池子ID | 项目中的数据池标识 | 71383, 71389 |
| 用户名 | Rosetta平台登录账号 | 15931113754 |
| 密码 | Rosetta平台登录密码 | Rosetta@0103 |

### 高级选项

- **池子类型**：选择处理完成池还是抽查池的数据
  - **完成池**：处理已完成的池子数据（check_pool=false）
  - **抽查池**：处理抽查的池子数据（check_pool=true）
- **测试模式**：启用后跳过数据下载步骤，直接使用本地已有数据进行测试
- **启用拆帧**：控制是否执行帧提取操作，可以只下载数据不拆帧

## 文件结构

```
frame_extraction_streamlit/
├── app.py              # Streamlit主应用
├── config.py           # 配置管理
├── utils.py            # 工具函数
├── requirements.txt    # 依赖包列表
├── README.md          # 说明文档
└── src/               # 核心处理代码
    ├── pipeline.py    # 处理管道
    ├── downloader.py  # 下载器
    ├── extractor.py   # 帧提取器
    └── rosetta_client.py # Rosetta客户端
```

## 注意事项

1. **网络连接**：确保有稳定的网络连接以访问Rosetta平台
2. **账号权限**：确保账号具有访问相应项目和数据的权限
3. **存储空间**：确保有足够的磁盘空间存储下载的数据和提取的帧
4. **处理时间**：处理时间取决于数据量大小，请耐心等待

## 故障排除

### 常见问题

1. **连接失败**
   - 检查网络连接
   - 确认Rosetta平台地址正确
   - 验证账号密码

2. **权限错误**
   - 确认账号有项目访问权限
   - 检查项目ID和池子ID是否正确

3. **处理中断**
   - 检查磁盘空间
   - 查看错误日志
   - 重新尝试处理

### 获取帮助

如遇到问题，请检查控制台输出和Streamlit界面中的错误信息，根据提示进行相应的调整。

## 更新日志

- v1.1.0：新增池子类型选择功能，支持完成池和抽查池切换
- v1.0.0：初始版本，支持基本的项目下载和帧提取功能

## Streamlit Cloud 部署

### 部署步骤

1. **准备代码**
   - 确保代码已推送到GitHub仓库
   - 检查 `.streamlit/secrets.toml` 文件是否存在（本地测试用）

2. **配置Secrets**
   在Streamlit Cloud部署时，需要在应用设置中配置以下secrets：

   ```toml
   [rosetta_credentials]
   username = "your_rosetta_username"
   password = "your_rosetta_password"
   ```

3. **部署应用**
   - 访问 [share.streamlit.io](https://share.streamlit.io)
   - 连接GitHub仓库
   - 选择要部署的分支
   - 点击部署

### 账号配置说明

应用支持两种账号配置方式：

1. **Streamlit Cloud部署模式**（推荐）
   - 使用 `st.secrets` 读取账号信息
   - 账号信息通过Streamlit Cloud的secrets功能安全存储
   - 界面中隐藏账号管理功能

2. **本地开发模式**
   - 使用session_state存储账号信息
   - 通过界面输入和管理账号
   - 适合本地测试和开发

### 环境变量配置

在Streamlit Cloud的Advanced settings中添加：

```toml
[rosetta_credentials]
username = "15931113754"  # 替换为您的用户名
password = "Rosetta@0103"  # 替换为您的密码
```

### 部署注意事项

- 确保所有依赖包在 `requirements.txt` 中正确列出
- 检查网络访问权限，确保能访问Rosetta平台
- 建议在部署前先在本地测试通过
- 账号信息通过secrets安全存储，不会暴露在代码中