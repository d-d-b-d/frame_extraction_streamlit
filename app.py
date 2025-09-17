"""
Rosetta数据下载与拆帧工具 - Streamlit版本
一个基于Web界面的数据下载和帧提取工具
"""

import streamlit as st
import os
import sys
import time
import threading
from typing import Dict, Any, Optional

# 添加src目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from config import StreamlitConfig
from utils import (
    create_zip_archive, 
    get_directory_size, 
    format_file_size, 
    validate_inputs,
    cleanup_temp_files,
    create_zip_archive_in_memory
)
from src.memory_pipeline import MemoryExtractionPipeline
import yaml

# 页面配置
st.set_page_config(
    page_title="Rosetta数据下载与拆帧工具",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 初始化配置
config_manager = StreamlitConfig()

# 会话状态初始化
if 'processing' not in st.session_state:
    st.session_state.processing = False
if 'result' not in st.session_state:
    st.session_state.result = None
if 'error' not in st.session_state:
    st.session_state.error = None


def process_project_async(params: Dict[str, Any], progress_callback):
    """异步处理项目（内存版）"""
    try:
        # 创建内存配置（不写入文件）
        config = config_manager.create_memory_config(params)
        progress_callback(20, "配置准备完成")
        
        # 初始化内存管道
        pipeline = MemoryExtractionPipeline(config)
        progress_callback(40, "处理管道初始化完成")
        
        # 处理项目
        result = pipeline.process_single_project()
        progress_callback(80, "项目处理完成")
        
        # 创建结果ZIP文件
        zip_data = pipeline.create_result_zip(result)
        progress_callback(90, "结果打包完成")
        
        # 计算大小
        size = len(zip_data)
        
        progress_callback(100, "处理完成")
        
        return {
            'project_id': result['project_id'],
            'files': result.get('files', {}),
            'zip_data': zip_data,
            'status': result['status'],
            'size': format_file_size(size),
            'frame_extraction': result.get('frame_extraction', False),
            'message': result.get('message', '处理完成')
        }
        
    except Exception as e:
        raise Exception(f"项目处理失败: {str(e)}")


def is_streamlit_cloud():
    """检测是否在Streamlit Cloud环境中运行"""
    # 检查Streamlit Cloud环境变量
    cloud_env = os.environ.get('STREAMLIT_CLOUD_RUNTIME', '').lower()
    if cloud_env == 'true':
        return True
    
    # 检查是否有Streamlit Cloud特定的环境变量
    if os.environ.get('STREAMLIT_SERVER_PORT'):
        # 检查是否存在secrets.toml文件
        secrets_paths = [
            os.path.expanduser("~/.streamlit/secrets.toml"),
            ".streamlit/secrets.toml"
        ]
        for path in secrets_paths:
            if os.path.exists(path):
                return True
    
    # 检查是否在容器中运行（Streamlit Cloud通常使用容器）
    if os.path.exists('/.dockerenv') or os.path.exists('/proc/1/cgroup'):
        return True
    
    return False

def get_credentials():
    """获取账号信息，仅从Streamlit Cloud的st.secrets读取"""
    # 检查是否在Streamlit Cloud环境中运行
    if not is_streamlit_cloud():
        raise Exception("此应用设计为仅在Streamlit Cloud中运行，本地运行需要配置secrets.toml文件")
    
    try:
        # 从st.secrets读取账号信息（仅在Streamlit Cloud中可用）
        username = st.secrets["rosetta_credentials"]["username"]
        password = st.secrets["rosetta_credentials"]["password"]
        return username, password
    except (KeyError, FileNotFoundError) as e:
        # 如果没有配置st.secrets，显示错误信息
        st.error("❌ 账号信息未配置")
        st.info("🔧 请在Streamlit Cloud的Secrets设置中配置账号信息")
        st.markdown("""
        **配置步骤：**
        1. 在Streamlit Cloud中打开应用设置
        2. 点击"Secrets"选项卡
        3. 添加以下TOML格式配置：
        ```toml
        [rosetta_credentials]
        username = "您的Rosetta用户名"
        password = "您的Rosetta密码"
        ```
        4. 保存并重新部署
        """)
        raise Exception("账号信息未在Streamlit Cloud中配置，无法继续操作")


def main():
    """主应用函数"""
    
    # 标题和描述
    st.title("Rosetta数据下载与拆帧工具")
    st.markdown("""
    这是一个基于Web的数据下载和帧提取工具，支持从Rosetta平台下载项目数据并进行帧提取。
    请在下方输入相关参数，然后点击**开始处理**按钮。
    """)
    
    # 侧边栏
    with st.sidebar:
        st.header("📋 使用说明")
        st.markdown("""
        **步骤说明：**
        1. 输入项目ID和池子ID
        2. 填写Rosetta账号信息
        3. 选择处理选项
        4. 点击开始处理
        5. 等待处理完成
        6. 下载结果文件
        
        **注意事项：**
        - 确保网络连接正常
        - 账号具有相应权限
        - 处理时间取决于数据量
        """)
        
        st.header("⚙️ 高级选项")
        
        # 池子类型选择
        pool_type = st.radio("池子类型", 
                           options=["完成池", "抽查池"],
                           index=0,
                           help="选择要处理的池子类型：完成池或抽查池")
        check_pool = pool_type == "抽查池"
        
        test_mode = st.checkbox("测试模式", value=False, 
                               help="跳过数据下载，使用已有数据")
        enable_extraction = st.checkbox("启用拆帧", value=True,
                                       help="是否执行帧提取操作")
        
        # 账号信息显示
        if is_streamlit_cloud():
            # Streamlit Cloud模式
            try:
                # 检查是否使用st.secrets
                st.secrets["rosetta_credentials"]
                st.info("🔐 账号信息已通过Streamlit Cloud配置")
                with st.expander("ℹ️ 账号配置说明", expanded=False):
                    st.markdown("""
                    **当前配置状态：**
                    ✅ 账号信息已配置在Streamlit Cloud中
                    
                    **如需修改账号：**
                    1. 在Streamlit Cloud中打开应用设置
                    2. 点击"Secrets"选项卡
                    3. 更新rosetta_credentials配置
                    4. 保存并重新部署
                    """)
            except (KeyError, FileNotFoundError):
                st.warning("⚠️ 账号信息未在Streamlit Cloud中配置")
                with st.expander("🔧 账号配置指导", expanded=True):
                    st.markdown("""
                    **配置步骤：**
                    1. 在Streamlit Cloud中打开应用设置
                    2. 点击"Secrets"选项卡
                    3. 添加以下TOML格式配置：
                    ```toml
                    [rosetta_credentials]
                    username = "您的Rosetta用户名"
                    password = "您的Rosetta密码"
                    ```
                    4. 保存并重新部署
                    """)
        else:
            # 本地运行模式
            st.warning("⚠️ 此应用设计为仅在Streamlit Cloud中运行")
            with st.expander("🔧 部署指导", expanded=False):
                st.markdown("""
                **本地运行说明：**
                此应用专为Streamlit Cloud设计，如需本地运行：
                
                **选项1：创建本地secrets.toml文件**
                在`.streamlit/secrets.toml`中添加：
                ```toml
                [rosetta_credentials]
                username = "您的Rosetta用户名"
                password = "您的Rosetta密码"
                ```
                
                **选项2：部署到Streamlit Cloud**
                1. 将代码推送到Git仓库
                2. 在Streamlit Cloud中创建新应用
                3. 在应用设置中配置Secrets
                """)
    
    # 主要内容区域
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("参数设置")
        
        # 项目信息输入
        with st.form("project_form"):
            st.subheader("项目信息")
            
            col_proj1, col_proj2 = st.columns(2)
            with col_proj1:
                project_id = st.text_input("项目ID", 
                                           help="请输入要处理的项目ID")
            
            with col_proj2:
                project_name = st.text_input("项目名称", 
                                           help="项目的显示名称")
            
            # 池子ID输入
            pool_ids_input = st.text_area("池子ID列表",
                                        help="每行输入一个池子ID")
            
            # 提交按钮
            submitted = st.form_submit_button("开始处理", 
                                            use_container_width=True,
                                            disabled=st.session_state.processing)
    
    with col2:
        st.header("处理状态")
        
        # 进度条
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # 结果显示区域
        result_container = st.container()
    
    # 处理表单提交
    if submitted and not st.session_state.processing:
        # 首先检查是否在Streamlit Cloud环境中
        if not is_streamlit_cloud():
            st.error("❌ 此应用设计为仅在Streamlit Cloud中运行")
            st.info("🔧 请部署到Streamlit Cloud或在本地创建.secrets.toml文件")
            return
        
        # 检查账号配置
        try:
            # 验证Streamlit Cloud配置
            st.secrets["rosetta_credentials"]
        except (KeyError, FileNotFoundError):
            st.error("❌ 无法开始处理：账号信息未在Streamlit Cloud中配置")
            st.info("🔧 请先在Streamlit Cloud中配置账号信息")
            return
        
        # 验证输入
        try:
            pool_ids = [int(x.strip()) for x in pool_ids_input.strip().split('\n') if x.strip()]
        except ValueError:
            st.error("池子ID格式错误，请确保每行都是数字")
            return
        
        # 获取账号信息（仅从Streamlit Cloud）
        try:
            username, password = get_credentials()
        except Exception as e:
            st.error(f"❌ 获取账号信息失败：{str(e)}")
            return
        
        # 验证输入
        error = validate_inputs(project_id, pool_ids, username, password)
        if error:
            st.error(error)
            return
        
        # 准备参数
        params = {
            'project_id': project_id,
            'pool_ids': pool_ids,
            'project_name': project_name,
            'username': username,
            'password': password,
            'test_mode': test_mode,
            'enable_extraction': enable_extraction,
            'check_pool': check_pool
        }
        
        # 开始处理
        st.session_state.processing = True
        st.session_state.result = None
        st.session_state.error = None
        
        # 重置进度条
        progress_bar.progress(0)
        status_text.text("准备开始...")
        
        # 处理函数
        def update_progress(percent, message):
            progress_bar.progress(percent / 100)
            status_text.text(f"{percent}% - {message}")
        
        try:
            # 处理项目
            result = process_project_async(params, update_progress)
            st.session_state.result = result
            status_text.text("✅ 处理完成！")
            
        except Exception as e:
            st.session_state.error = str(e)
            status_text.text(f"❌ 处理失败: {str(e)}")
            
        finally:
            st.session_state.processing = False
    
    # 显示结果
    with result_container:
        if st.session_state.result:
            result = st.session_state.result
            
            st.success("🎉 项目处理成功！")
            
            # 结果信息
            with st.expander("📋 处理结果", expanded=True):
                st.write(f"**项目ID:** {result['project_id']}")
                st.write(f"**状态:** {result['status']}")
                st.write(f"**大小:** {result.get('size', '未知')}")
                st.write(f"**拆帧状态:** {'已启用' if result.get('frame_extraction') else '未启用'}")
            
            # 文件列表
            if result.get('files'):
                with st.expander("📁 文件列表"):
                    for filename, data in result['files'].items():
                        st.write(f"📄 {filename} ({format_file_size(len(data))})")
            
            # 下载按钮
            if result.get('zip_data'):
                st.download_button(
                    label=f"📥 下载结果 ZIP 文件 ({format_file_size(len(result['zip_data']))})",
                    data=result['zip_data'],
                    file_name=f"project_{result['project_id']}_results.zip",
                    mime="application/zip",
                    use_container_width=True
                )
                
                st.info("💡 下载完成后，解压即可查看所有处理结果")
            else:
                st.warning("⚠️ 未找到处理结果数据")
        
        elif st.session_state.error:
            st.error(f"❌ 处理失败: {st.session_state.error}")
    
    # 处理状态指示器
    if st.session_state.processing:
        with st.spinner("正在处理项目，请稍候..."):
            # 这里可以添加实时日志显示
            pass


if __name__ == "__main__":
    main()