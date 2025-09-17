"""
Rosetta数据下载器
基于get_rosetta_json.py重构，提供更清晰的接口
"""

import os
import yaml
import shutil
from typing import List, Optional
from .rosetta_client import GetRosData


class RosettaDownloader:
    """Rosetta数据下载器"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """初始化下载器
        
        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path
        self.config = self._load_config()
        
    def _load_config(self) -> dict:
        """加载配置文件"""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def download_project_data(self, 
                            project_id: int = None, 
                            pool_ids: List[int] = None,
                            save_path: str = None) -> str:
        """下载项目数据
        
        Args:
            project_id: 项目ID，如果为None则使用配置文件中的值
            pool_ids: 池子ID列表，如果为None则使用配置文件中的值
            save_path: 保存路径，如果为None则使用配置文件中的值
            
        Returns:
            str: 数据保存路径
        """
        # 使用配置或参数
        project_id = project_id or self.config['project']['project_id']
        pool_ids = pool_ids or self.config['project']['pool_ids']
        save_path = save_path or self.config['download']['save_path']
        
        # 确保目录存在
        os.makedirs(save_path, exist_ok=True)
        
        # 构建项目路径
        project_path = os.path.join(save_path, str(project_id))
        
        # 如果已存在，先删除
        if os.path.exists(project_path):
            shutil.rmtree(project_path)
            print(f'已删除目录：{project_path}')
        
        print(f'开始下载项目 {project_id} 的数据...')
        
        # 获取Rosetta配置
        rosetta_config = self.config.get('rosetta', {})
        username = rosetta_config.get('username')
        password = rosetta_config.get('password')
        
        # 验证认证信息
        if not username or not password:
            raise ValueError(
                "未配置Rosetta认证信息！\n"
                "请在config.yaml中添加：\n"
                "rosetta:\n"
                "  username: \"你的用户名\"\n"
                "  password: \"你的密码\"\n"
                "\n"
                "或者设置环境变量：\n"
                "export ROSETTA_USERNAME=你的用户名\n"
                "export ROSETTA_PASSWORD=你的密码"
            )
        
        # 使用GetRosData下载数据
        downloader = GetRosData(
            project_id=project_id,
            pool_id=pool_ids,
            save_path=save_path,
            _type=self.config['download']['download_type'],
            is_check_pool=self.config['download']['check_pool'],
            username=username,
            password=password
        )
        
        downloader.get_unziped_data()
        
        print(f'数据下载完成，保存在：{project_path}')
        return project_path
    
    def download_multiple_projects(self, projects: List[dict]) -> List[str]:
        """批量下载多个项目
        
        Args:
            projects: 项目列表，每个项目包含project_id和pool_ids
            
        Returns:
            List[str]: 所有下载的项目路径列表
        """
        project_paths = []
        
        for project in projects:
            project_id = project['project_id']
            pool_ids = project['pool_ids']
            save_path = project.get('save_path') or self.config['download']['save_path']
            
            path = self.download_project_data(project_id, pool_ids, save_path)
            project_paths.append(path)
        
        return project_paths