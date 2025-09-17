"""
帧数据提取器
基于stardust.rosetta重构，提供统一的拆帧接口
"""

import os
import yaml
from typing import Optional
from .frame_splitter import to_split, to_split_new


class FrameExtractor:
    """帧数据提取器"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """初始化提取器
        
        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path
        self.config = self._load_config()
        
    def _load_config(self) -> dict:
        """加载配置文件"""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def extract_frames(self, 
                      project_path: str,
                      method: str = None) -> str:
        """执行拆帧操作
        
        Args:
            project_path: 项目数据路径
            method: 拆帧方法，可选 'rosetta' 或 'rosetta_new'，默认为配置中的值
            
        Returns:
            str: 拆帧后的数据路径（与输入路径相同）
        """
        method = method or self.config['frame_extraction']['method']
        
        if not os.path.exists(project_path):
            raise FileNotFoundError(f"项目路径不存在：{project_path}")
        
        print(f'开始使用 {method} 方法拆帧...')
        
        if method == "rosetta":
            to_split(project_path)
        elif method == "rosetta_new":
            to_split_new(project_path)
        else:
            raise ValueError(f"不支持的拆帧方法：{method}")
        
        print(f'拆帧完成：{project_path}')
        return project_path
    
    def get_project_structure(self, project_path: str) -> dict:
        """获取项目结构信息
        
        Args:
            project_path: 项目路径
            
        Returns:
            dict: 包含项目结构信息的字典
        """
        if not os.path.exists(project_path):
            return {}
        
        structure = {
            'path': project_path,
            'exists': True,
            'json_files': [],
            'subdirectories': []
        }
        
        # 统计JSON文件
        for root, dirs, files in os.walk(project_path):
            for file in files:
                if file.endswith('.json'):
                    structure['json_files'].append(
                        os.path.relpath(os.path.join(root, file), project_path)
                    )
            for dir in dirs:
                full_dir = os.path.join(root, dir)
                structure['subdirectories'].append(
                    os.path.relpath(full_dir, project_path)
                )
        
        return structure