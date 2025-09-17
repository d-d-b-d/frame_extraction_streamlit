"""
拆帧处理管道
整合下载和拆帧功能，提供一站式处理
"""

import os
import yaml
import time
from typing import Optional, List, Dict
from .downloader import RosettaDownloader
from .extractor import FrameExtractor


class ExtractionPipeline:
    """拆帧处理管道"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """初始化管道
        
        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.downloader = RosettaDownloader(config_path)
        self.extractor = FrameExtractor(config_path)
    
    def _load_config(self) -> dict:
        """加载配置文件"""
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def process_single_project(self, 
                             project_id: int = None,
                             pool_ids: List[int] = None,
                             project_name: str = None) -> Dict[str, str]:
        """处理单个项目
        
        Args:
            project_id: 项目ID
            pool_ids: 池子ID列表
            project_name: 项目中文名
            
        Returns:
            Dict[str, str]: 包含处理结果的字典
        """
        # 使用配置或参数
        project_id = project_id or self.config['project']['project_id']
        pool_ids = pool_ids or self.config['project']['pool_ids']
        project_name = project_name or self.config['project'].get('project_name_cn') or str(project_id)
        
        # 调试模式检查
        if self.config['debug']['test_mode']:
            print("【测试模式】跳过数据下载，使用已有数据")
            project_path = os.path.join(
                self.config['download']['save_path'], 
                str(project_id)
            )
            if not os.path.exists(project_path):
                raise FileNotFoundError(f"测试模式下路径不存在：{project_path}")
        else:
            # 下载数据
            project_path = self.downloader.download_project_data(
                project_id=project_id,
                pool_ids=pool_ids
            )
        
        # 检查是否启用拆帧
        if not self.config['frame_extraction']['enabled']:
            print("拆帧功能已禁用，跳过拆帧步骤")
            return {
                'project_id': str(project_id),
                'project_path': project_path,
                'frame_extraction': False,
                'status': 'completed_without_extraction'
            }
        
        # 执行拆帧
        extracted_path = self.extractor.extract_frames(project_path)
        
        # 构建导出路径
        if self.config['frame_extraction']['output']['add_timestamp']:
            time_str = time.strftime('%Y%m%d_%H%M%S')
            export_dir = f"{project_name}-{project_id}_{time_str}"
        else:
            export_dir = f"{project_name}-{project_id}"
        
        export_path = os.path.join(
            self.config['frame_extraction']['output']['export_prefix'],
            self.config['frame_extraction']['output']['export_subdir'],
            export_dir
        )
        
        return {
            'project_id': str(project_id),
            'project_path': extracted_path,
            'export_path': export_path,
            'frame_extraction': True,
            'status': 'completed'
        }
    
    def process_multiple_projects(self, projects: List[Dict]) -> List[Dict[str, str]]:
        """批量处理多个项目
        
        Args:
            projects: 项目列表，每个项目包含project_id, pool_ids, project_name
            
        Returns:
            List[Dict[str, str]]: 所有项目的处理结果
        """
        results = []
        
        for i, project in enumerate(projects, 1):
            print(f"\n【{i}/{len(projects)}】处理项目：{project['project_name']} (ID: {project['project_id']})")
            
            try:
                result = self.process_single_project(
                    project_id=project['project_id'],
                    pool_ids=project['pool_ids'],
                    project_name=project['project_name']
                )
                results.append(result)
                print(f"✅ 项目 {project['project_id']} 处理完成")
            except Exception as e:
                print(f"❌ 项目 {project['project_id']} 处理失败：{str(e)}")
                results.append({
                    'project_id': str(project['project_id']),
                    'error': str(e),
                    'status': 'failed'
                })
        
        return results
    
    def get_project_info(self, project_path: str) -> Dict:
        """获取项目详细信息
        
        Args:
            project_path: 项目路径
            
        Returns:
            Dict: 项目详细信息
        """
        structure = self.extractor.get_project_structure(project_path)
        
        info = {
            'path': project_path,
            'exists': structure.get('exists', False),
            'json_file_count': len(structure.get('json_files', [])),
            'subdirectories': structure.get('subdirectories', []),
            'json_files': structure.get('json_files', [])
        }
        
        return info