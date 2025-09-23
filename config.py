"""
Streamlit应用配置管理
"""

import os
import yaml
from typing import Dict, Any


class StreamlitConfig:
    """Streamlit应用配置管理器"""
    
    def __init__(self):
        # 配置管理器现在只负责内存配置，不再需要文件目录
        pass
        

    
    def create_config(self, params: Dict[str, Any]) -> str:
        """
        根据用户输入创建配置文件
        
        Args:
            params: 用户输入的参数
            
        Returns:
            str: 配置文件路径
        """
        config_data = self._build_config_data(params)
        
        # 生成配置文件路径
        config_filename = f"config_{params['project_id']}_{int(params.get('timestamp', 0))}.yaml"
        config_path = os.path.join(self.config_dir, config_filename)
        
        # 保存配置文件
        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(config_data, f, default_flow_style=False, allow_unicode=True)
            
        return config_path
    
    def create_memory_config(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        创建内存配置（不写入文件）
        
        Args:
            params: 用户输入的参数
            
        Returns:
            Dict[str, Any]: 配置字典
        """
        return self._build_config_data(params)
    
    def _build_config_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        构建配置数据
        
        Args:
            params: 用户输入的参数
            
        Returns:
            Dict[str, Any]: 配置字典
        """
        return {
            'debug': {
                'log_level': 'INFO',
                'test_mode': params.get('test_mode', False)
            },
            'download': {
                'check_pool': params.get('check_pool', False),
                'download_type': params.get('download_type', 1),
                'save_path': None  # 内存处理，不需要保存路径
            },
            'frame_extraction': {
                'enabled': params.get('enable_extraction', True),
                'method': 'rosetta',
                'output': {
                    'add_timestamp': True,
                    'export_prefix': None,  # 内存处理，不需要输出前缀
                    'export_subdir': None   # 内存处理，不需要输出子目录
                }
            },
            'project': {
                'project_id': params['project_id'],
                'pool_ids': params['pool_ids'],
                'project_name_cn': params.get('project_name', f"项目{params['project_id']}")
            },
            'rosetta': {
                'env_dev': 'https://dev-server.rosettalab.top',
                'env_prod': 'https://server.rosettalab.top',
                'username': params['username'],
                'password': params['password']
            }
        }
    
    def get_default_params(self) -> Dict[str, Any]:
        """获取默认参数"""
        return {
            'project_id': 3603,
            'pool_ids': [71383, 71389, 71394, 71399],
            'project_name': '测试项目',
            'username': 'your_username',
            'password': 'your_password',
            'test_mode': False,
            'enable_extraction': True,
            'check_pool': False,  # 默认使用完成池
            'download_type': 1  # 默认任务格式导出
        }