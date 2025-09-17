"""
Streamlit应用工具函数
"""

import os
import zipfile
import shutil
import io
import json
from typing import List, Optional, Dict, Any
import streamlit as st


def create_zip_archive(source_dir: str, output_path: str) -> bool:
    """
    创建ZIP压缩包（文件系统版本）
    
    Args:
        source_dir: 源目录路径
        output_path: 输出ZIP文件路径
        
    Returns:
        bool: 是否成功
    """
    try:
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(source_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, source_dir)
                    zipf.write(file_path, arcname)
        return True
    except Exception as e:
        st.error(f"创建压缩包失败: {str(e)}")
        return False


def create_zip_archive_in_memory(data_dict: Dict[str, Any]) -> bytes:
    """
    在内存中创建ZIP压缩包
    
    Args:
        data_dict: 包含文件数据的字典，格式为 {文件路径: 文件内容}
        
    Returns:
        bytes: ZIP文件的二进制数据
    """
    try:
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path, file_content in data_dict.items():
                # 确保文件内容是字节类型
                if isinstance(file_content, str):
                    file_content = file_content.encode('utf-8')
                elif isinstance(file_content, dict):
                    file_content = json.dumps(file_content, ensure_ascii=False, indent=2).encode('utf-8')
                
                zipf.writestr(file_path, file_content)
        
        return zip_buffer.getvalue()
    except Exception as e:
        st.error(f"创建内存压缩包失败: {str(e)}")
        return b''


def create_zip_from_folder_in_memory(folder_path: str) -> bytes:
    """
    从文件夹在内存中创建ZIP压缩包
    
    Args:
        folder_path: 文件夹路径
        
    Returns:
        bytes: ZIP文件的二进制数据
    """
    try:
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, folder_path)
                    
                    # 读取文件内容
                    try:
                        with open(file_path, 'rb') as f:
                            file_content = f.read()
                        zipf.writestr(arcname, file_content)
                    except Exception as e:
                        st.warning(f"跳过文件 {file_path}: {str(e)}")
        
        return zip_buffer.getvalue()
    except Exception as e:
        st.error(f"创建文件夹内存压缩包失败: {str(e)}")
        return b''


def get_directory_size(directory: str) -> int:
    """
    获取目录大小（字节）
    
    Args:
        directory: 目录路径
        
    Returns:
        int: 目录大小（字节）
    """
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                if os.path.exists(file_path):
                    total_size += os.path.getsize(file_path)
    except Exception:
        pass
    return total_size


def format_file_size(size_bytes: int) -> str:
    """
    格式化文件大小
    
    Args:
        size_bytes: 字节数
        
    Returns:
        str: 格式化后的大小字符串
    """
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def cleanup_temp_files(config_path: str, output_dir: str, max_age_hours: int = 24):
    """
    清理临时文件
    
    Args:
        config_path: 配置文件路径
        output_dir: 输出目录路径
        max_age_hours: 最大保留时间（小时）
    """
    import time
    
    current_time = time.time()
    max_age_seconds = max_age_hours * 3600
    
    # 清理配置文件
    try:
        if os.path.exists(config_path):
            file_age = current_time - os.path.getmtime(config_path)
            if file_age > max_age_seconds:
                os.remove(config_path)
    except Exception:
        pass
    
    # 清理输出目录中的旧文件
    try:
        if os.path.exists(output_dir):
            for item in os.listdir(output_dir):
                item_path = os.path.join(output_dir, item)
                if os.path.isfile(item_path):
                    file_age = current_time - os.path.getmtime(item_path)
                    if file_age > max_age_seconds:
                        os.remove(item_path)
                elif os.path.isdir(item_path):
                    dir_age = current_time - os.path.getmtime(item_path)
                    if dir_age > max_age_seconds:
                        shutil.rmtree(item_path)
    except Exception:
        pass


def validate_inputs(project_id: str, pool_ids: List[int], username: str, password: str) -> Optional[str]:
    """
    验证用户输入
    
    Args:
        project_id: 项目ID（字符串格式）
        pool_ids: 池子ID列表
        username: 用户名
        password: 密码
        
    Returns:
        Optional[str]: 错误信息，如果验证通过则返回None
    """
    if not project_id or not project_id.strip():
        return "项目ID不能为空"
    
    # 尝试将项目ID转换为整数进行验证
    try:
        project_id_int = int(project_id)
        if project_id_int <= 0:
            return "项目ID必须为正整数"
    except ValueError:
        return "项目ID必须为数字"
    
    if not pool_ids:
        return "池子ID不能为空"
    
    for pool_id in pool_ids:
        if not pool_id or pool_id <= 0:
            return "池子ID必须为正整数"
    
    if not username or not username.strip():
        return "用户名不能为空"
    
    if not password or not password.strip():
        return "密码不能为空"
    
    return None