"""
内存版拆帧处理管道
支持在内存中完成下载和拆帧，不依赖本地文件系统
"""

import io
import json
from typing import Dict, Any, Optional
from .memory_client import MemoryRosettaClient, MemoryFrameExtractor
from .smart_memory_client import SmartMemoryRosettaClient


class MemoryExtractionPipeline:
    """内存版拆帧处理管道"""
    
    def __init__(self, config: Dict[str, Any]):
        """初始化管道
        
        Args:
            config: 配置字典
        """
        self.config = config
        
        # 根据配置选择使用智能客户端还是普通内存客户端
        if config['download'].get('smart_download', True):
            print("🧠 使用智能下载模式（支持自动故障转移）")
            self.downloader = SmartMemoryRosettaClient(
                project_id=config['project']['project_id'],
                pool_id=config['project']['pool_ids'],
                _type=config['download']['download_type'],
                is_check_pool=config['download']['check_pool'],
                username=config['rosetta']['username'],
                password=config['rosetta']['password']
            )
        else:
            print("📦 使用标准内存下载模式")
            self.downloader = MemoryRosettaClient(
                project_id=config['project']['project_id'],
                pool_id=config['project']['pool_ids'],
                _type=config['download']['download_type'],
                is_check_pool=config['download']['check_pool'],
                username=config['rosetta']['username'],
                password=config['rosetta']['password']
            )
        
        self.extractor = MemoryFrameExtractor(config)
    
    def process_single_project(self, 
                             project_id: int = None,
                             pool_ids: list = None,
                             project_name: str = None) -> Dict[str, Any]:
        """处理单个项目
        
        Args:
            project_id: 项目ID
            pool_ids: 池子ID列表
            project_name: 项目中文名
            
        Returns:
            Dict[str, Any]: 包含处理结果的字典
        """
        # 使用配置或参数
        project_id = project_id or self.config['project']['project_id']
        pool_ids = pool_ids or self.config['project']['pool_ids']
        project_name = project_name or self.config['project'].get('project_name_cn') or str(project_id)
        
        # 调试模式检查
        if self.config['debug']['test_mode']:
            print("【测试模式】跳过数据下载，使用模拟数据")
            # 返回模拟数据
            return {
                'project_id': str(project_id),
                'files': self._generate_test_data(),
                'frame_extraction': self.config['frame_extraction']['enabled'],
                'status': 'completed_test_mode',
                'message': '测试模式：使用模拟数据'
            }
        
        # 下载数据到内存
        print(f"开始下载项目 {project_id} 的数据到内存...")
        try:
            files_dict = self.downloader.get_project_data_to_memory()
            print(f"数据下载完成，共 {len(files_dict)} 个文件")
        except Exception as e:
            error_msg = str(e)
            if "504" in error_msg or "Gateway Time-out" in error_msg:
                print(f"❌ 网关超时错误 (504): {error_msg}")
                return {
                    'project_id': str(project_id),
                    'error': error_msg,
                    'status': 'failed_gateway_timeout',
                    'message': '下载超时：网关错误 (504)，请稍后重试或检查网络连接'
                }
            elif "所有下载接口都失败" in error_msg:
                print(f"❌ 所有下载接口都失败: {error_msg}")
                return {
                    'project_id': str(project_id),
                    'error': error_msg,
                    'status': 'failed_all_interfaces',
                    'message': '所有下载接口都失败，请检查项目ID、池子ID和网络连接'
                }
            else:
                print(f"❌ 数据下载失败: {error_msg}")
                return {
                    'project_id': str(project_id),
                    'error': error_msg,
                    'status': 'failed_download',
                    'message': f'数据下载失败: {error_msg}'
                }
        
        # 检查是否启用拆帧
        if not self.config['frame_extraction']['enabled']:
            print("拆帧功能已禁用，跳过拆帧步骤")
            return {
                'project_id': str(project_id),
                'files': files_dict,
                'frame_extraction': False,
                'status': 'completed_without_extraction',
                'message': '处理完成（未启用拆帧）'
            }
        
        # 执行拆帧（在内存中）
        print("开始在内存中执行拆帧...")
        processed_files = self.extractor.extract_frames_from_memory(files_dict)
        print(f"拆帧完成，共 {len(processed_files)} 个文件")
        
        return {
            'project_id': str(project_id),
            'files': processed_files,
            'frame_extraction': True,
            'status': 'completed',
            'message': '处理完成'
        }
    
    def process_multiple_projects(self, projects: list) -> list:
        """批量处理多个项目
        
        Args:
            projects: 项目列表，每个项目包含project_id, pool_ids, project_name
            
        Returns:
            list: 所有项目的处理结果
        """
        results = []
        
        for i, project in enumerate(projects, 1):
            print(f"\n【{i}/{len(projects)}】处理项目：{project['project_name']} (ID: {project['project_id']})")
            
            try:
                # 更新下载器配置
                self.downloader.project_id = project['project_id']
                self.downloader.pool_id = project['pool_ids']
                
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
                    'status': 'failed',
                    'message': f'处理失败: {str(e)}'
                })
        
        return results
    
    def _generate_test_data(self) -> Dict[str, bytes]:
        """生成测试数据
        
        Returns:
            Dict[str, bytes]: 测试文件数据
        """
        # 生成模拟的JSON数据
        test_json = {
            "project_info": {
                "project_id": self.config['project']['project_id'],
                "project_name": self.config['project'].get('project_name_cn', '测试项目'),
                "created_time": "2024-01-01 12:00:00"
            },
            "frames": [
                {
                    "frame_id": f"frame_{i:06d}",
                    "timestamp": f"00:00:{i:02d}.000",
                    "key_frame": i % 5 == 0,
                    "annotations": []
                }
                for i in range(1, 11)
            ],
            "total_frames": 10
        }
        
        json_content = json.dumps(test_json, ensure_ascii=False, indent=2).encode('utf-8')
        
        return {
            "project_data.json": json_content,
            "README.txt": "这是测试模式下的模拟数据".encode('utf-8'),
            "metadata.json": json.dumps({"version": "1.0", "mode": "test"}, indent=2).encode('utf-8')
        }

    
 
    def create_result_zip(self, result: Dict[str, Any]) -> bytes:
        """将处理结果创建为ZIP文件
        
        Args:
            result: 处理结果
            
        Returns:
            bytes: ZIP文件的二进制数据
        """
        from utils import create_zip_archive_in_memory
        
        if 'files' in result:
            # 直接使用文件数据创建ZIP，保持原始文件结构
            return create_zip_archive_in_memory(result['files'])
        else:
            # 如果没有文件数据，创建包含结果信息的ZIP
            result_data = {
                "result_info.json": json.dumps({
                    "project_id": result.get('project_id'),
                    "status": result.get('status'),
                    "message": result.get('message'),
                    "frame_extraction": result.get('frame_extraction', False)
                }, ensure_ascii=False, indent=2).encode('utf-8')
            }
            return create_zip_archive_in_memory(result_data)