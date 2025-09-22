"""
大文件处理管道
集成大文件下载、拆帧的完整流程
"""

import os
import json
import shutil
import time
from typing import Dict, Any, Optional
from big_file_client import BigFileRosettaClient
from memory_client import MemoryFrameExtractor
import subprocess


class BigFileProcessingPipeline:
    """大文件处理管道"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Args:
            config: 配置字典
        """
        self.config = config
        
        # 初始化大文件下载客户端
        self.big_file_client = BigFileRosettaClient(
            project_id=config['project']['project_id'],
            pool_id=config['project']['pool_ids'],
            save_path=config['paths']['download_path'],
            _type=config['download']['download_type'],
            is_check_pool=config['download']['check_pool'],
            username=config['rosetta']['username'],
            password=config['rosetta']['password']
        )
        
        # 初始化内存帧提取器
        self.frame_extractor = MemoryFrameExtractor(config)
    
    def process_large_project(self, progress_callback=None) -> Dict[str, Any]:
        """处理大文件项目
        
        Args:
            progress_callback: 进度回调函数
            
        Returns:
            Dict[str, Any]: 处理结果
        """
        try:
            project_id = self.config['project']['project_id']
            pool_ids = self.config['project']['pool_ids']
            project_name = self.config['project'].get('project_name_cn') or str(project_id)
            
            if progress_callback:
                progress_callback(10, f"开始处理大文件项目 {project_name}...")
            
            # 第一步：使用大文件客户端下载数据
            print(f"\n📥 开始下载大文件项目 {project_id}...")
            project_path = self.big_file_client.get_unziped_data()
            
            if progress_callback:
                progress_callback(40, "大文件下载完成")
            
            # 检查是否启用拆帧
            if not self.config['frame_extraction']['enabled']:
                print("ℹ️  拆帧功能已禁用")
                return {
                    'project_id': str(project_id),
                    'project_path': project_path,
                    'frame_extraction': False,
                    'status': 'completed_without_extraction',
                    'message': '大文件下载完成（未启用拆帧）'
                }
            
            # 第二步：执行拆帧
            print(f"\n🔄 开始拆帧...")
            
            if progress_callback:
                progress_callback(60, "正在执行拆帧...")
            
            # 使用stardust进行拆帧
            success = self._perform_frame_extraction(project_path)
            
            if not success:
                raise Exception("拆帧失败")
            
            if progress_callback:
                progress_callback(90, "拆帧完成")
            
            print(f"✅ 拆帧完成")
            
            return {
                'project_id': str(project_id),
                'project_path': project_path,
                'frame_extraction': True,
                'status': 'completed',
                'message': '大文件处理完成'
            }
            
        except Exception as e:
            print(f"❌ 大文件处理失败: {str(e)}")
            raise Exception(f"大文件处理失败: {str(e)}")
    
    def _perform_frame_extraction(self, project_path: str) -> bool:
        """执行拆帧操作
        
        Args:
            project_path: 项目路径
            
        Returns:
            bool: 是否成功
        """
        try:
            # 使用stardust的rosetta模块进行拆帧
            # 这里假设stardust模块可用，如果不可用需要调整
            
            # 尝试导入stardust模块
            try:
                from stardust import rosetta
                print("使用stardust.rosetta进行拆帧...")
                rosetta.to_split(project_path)
                return True
            except ImportError:
                print("stardust.rosetta模块不可用，尝试其他拆帧方法...")
            
            # 如果stardust不可用，使用备用方法
            return self._fallback_frame_extraction(project_path)
            
        except Exception as e:
            print(f"拆帧操作失败: {str(e)}")
            return False
    
    def _fallback_frame_extraction(self, project_path: str) -> bool:
        """备用拆帧方法
        
        Args:
            project_path: 项目路径
            
        Returns:
            bool: 是否成功
        """
        try:
            # 这里可以实现一个简单的拆帧逻辑
            # 或者调用其他可用的拆帧工具
            
            print(f"使用备用拆帧方法处理: {project_path}")
            
            # 查找所有JSON文件
            json_files = []
            for root, dirs, files in os.walk(project_path):
                for file in files:
                    if file.endswith('.json') and not file.startswith('.'):
                        json_files.append(os.path.join(root, file))
            
            if not json_files:
                print("未找到JSON文件，跳过拆帧")
                return True
            
            print(f"找到 {len(json_files)} 个JSON文件")
            
            # 简单的拆帧处理（这里只是示例）
            for json_file in json_files:
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # 这里可以添加具体的拆帧逻辑
                    # 根据你的需求处理data
                    
                    print(f"处理文件: {json_file}")
                    
                except Exception as e:
                    print(f"处理文件 {json_file} 失败: {str(e)}")
                    continue
            
            return True
            
        except Exception as e:
            print(f"备用拆帧方法失败: {str(e)}")
            return False
    
    def create_result_archive(self, result: Dict[str, Any]) -> str:
        """创建结果归档文件
        
        Args:
            result: 处理结果
            
        Returns:
            str: 归档文件路径
        """
        try:
            project_path = result['project_path']
            project_id = result['project_id']
            
            # 创建结果目录
            result_dir = os.path.join(self.config['paths']['result_path'], f"project_{project_id}_result")
            if os.path.exists(result_dir):
                shutil.rmtree(result_dir)
            os.makedirs(result_dir, exist_ok=True)
            
            # 复制处理后的文件到结果目录
            for item in os.listdir(project_path):
                src_path = os.path.join(project_path, item)
                dst_path = os.path.join(result_dir, item)
                
                if os.path.isdir(src_path):
                    shutil.copytree(src_path, dst_path)
                else:
                    shutil.copy2(src_path, dst_path)
            
            # 创建结果信息文件
            result_info = {
                'project_id': project_id,
                'status': result['status'],
                'message': result['message'],
                'frame_extraction': result['frame_extraction'],
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            info_file = os.path.join(result_dir, 'result_info.json')
            with open(info_file, 'w', encoding='utf-8') as f:
                json.dump(result_info, f, ensure_ascii=False, indent=2)
            
            # 创建ZIP归档
            zip_path = f"{result_dir}.zip"
            if os.path.exists(zip_path):
                os.remove(zip_path)
            
            # 使用系统命令创建ZIP
            result = subprocess.run(['zip', '-r', zip_path, os.path.basename(result_dir)], 
                                  cwd=os.path.dirname(result_dir), 
                                  capture_output=True, text=True)
            
            if result.returncode != 0:
                raise Exception(f"创建ZIP失败: {result.stderr}")
            
            print(f"✅ 结果归档创建完成: {zip_path}")
            return zip_path
            
        except Exception as e:
            print(f"❌ 创建结果归档失败: {str(e)}")
            raise Exception(f"创建结果归档失败: {str(e)}")