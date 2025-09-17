"""
帧数据拆帧器
集成stardust的拆帧功能，提供独立的拆帧实现
"""

import os
import json
import numpy as np
from glob import glob
from tqdm import tqdm
from typing import Union, Optional, List, Dict, Any
import shutil


class Camera:
    """相机类，模拟stardust.components.camera.Camera"""
    
    def __init__(self, camera_type: str, heading: tuple, position: tuple, 
                 intrinsic: tuple, radial: tuple, tangential: tuple):
        self.type = camera_type
        self.heading = heading
        self.position = position
        self.intrinsic = intrinsic
        self.radial = radial
        self.tangential = tangential


class Slot:
    """插槽类"""
    
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def slot_structure(annotation: Dict[str, Any]) -> Dict[str, Any]:
    """创建空的插槽结构"""
    return {
        'id': annotation.get('id', ''),
        'type': annotation['type'],
        'key': annotation['key'],
        'label': annotation.get('label', ''),
        'color': annotation.get('color', ''),
        'slotsChildren': [],
        'childrenOnly': []
    }


def empty(obj):
    """检查对象是否为空"""
    if isinstance(obj, dict):
        return not obj
    if isinstance(obj, list):
        return not obj
    return obj is None


def frame_name(original_path: str, frame_number: int) -> str:
    """生成帧文件名"""
    base_name = os.path.basename(original_path)
    name_without_ext = os.path.splitext(base_name)[0]
    dir_name = os.path.dirname(original_path)
    return os.path.join(dir_name, f"{name_without_ext}_{frame_number:06d}.json")


class FrameSplitter:
    """帧数据拆帧器"""
    
    SEQUENCE_TYPES = [
        "IMAGE_SEQUENCE", 
        "IMAGE_SET_SEQUENCE", 
        "POINTCLOUD_SEQUENCE", 
        "POINTCLOUD_SET_SEQUENCE"
    ]
    
    def __init__(self):
        pass
    
    def load_json(self, json_path: str) -> Dict[str, Any]:
        """加载JSON文件"""
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def process_file(self, json_path: str) -> bool:
        """处理单个JSON文件"""
        try:
            data = self.load_json(json_path)
            
            # 检查是否是序列类型
            record = data.get('taskParams', {}).get('record', {})
            attachment_type = record.get('attachmentType', '')
            
            if attachment_type not in self.SEQUENCE_TYPES:
                print(f"跳过非序列文件: {json_path}")
                return True
            
            # 提取基本信息
            project_id = data.get('projectId', 0)
            dataset_id = data.get('datasetId', 0)
            pool_id = data.get('poolId', 0)
            task_id = data.get('taskId', 0)
            status = data.get('status', 0)
            
            # 获取附件信息
            attachment = record.get('attachment', [])
            attachment_length = len(attachment)
            metadata = record.get('metadata', {})
            operators = data.get('taskParams', {}).get('operators', [])
            
            # 获取结果信息
            result_annotations = data.get('result', {}).get('annotations', [])
            result_hints = data.get('result', {}).get('hints', [])
            result_metadata = data.get('result', {}).get('metadata', {})
            
            if attachment_length == 0:
                print(f"跳过无附件的文件: {json_path}")
                return True
            
            # 为每一帧创建新文件
            for frame_number in range(attachment_length):
                frame_data = self._create_frame_data(
                    project_id, dataset_id, pool_id, task_id, status,
                    attachment_type, attachment, metadata, operators,
                    result_annotations, result_hints, result_metadata,
                    frame_number
                )
                
                # 生成新文件路径
                new_path = frame_name(json_path, frame_number)
                task_dir = os.path.join(os.path.dirname(new_path), str(task_id))
                os.makedirs(task_dir, exist_ok=True)
                
                final_path = os.path.join(task_dir, os.path.basename(new_path))
                
                with open(final_path, 'w', encoding='utf-8') as f:
                    json.dump(frame_data, f, ensure_ascii=False, indent=2)
            
            # 删除原始文件
            os.remove(json_path)
            print(f"已处理: {json_path} -> {attachment_length} 帧")
            return True
            
        except Exception as e:
            print(f"处理文件失败 {json_path}: {str(e)}")
            return False
    
    def _create_frame_data(self, project_id: int, dataset_id: int, pool_id: int, 
                          task_id: int, status: int, attachment_type: str,
                          attachment: List[Dict], metadata: Dict, operators: List[Dict],
                          result_annotations: List[Dict], result_hints: List[Dict],
                          result_metadata: Dict, frame_number: int) -> Dict[str, Any]:
        """创建单帧数据"""
        
        # 创建空的标注结构
        frame_annotations = []
        for annotation in result_annotations:
            frame_annotation = self._create_empty_annotation(annotation)
            frame_annotations.append(frame_annotation)
        
        return {
            "projectId": project_id,
            "datasetId": dataset_id,
            "poolId": pool_id,
            "taskId": task_id,
            "status": status,
            "taskParams": {
                "record": {
                    "attachmentType": attachment_type.replace("_SEQUENCE", ""),
                    "attachment": attachment[frame_number] if frame_number < len(attachment) else {},
                    "metadata": metadata
                },
                "operators": operators
            },
            "result": {
                "annotations": frame_annotations,
                "hints": result_hints,
                "metadata": result_metadata
            }
        }
    
    def _create_empty_annotation(self, annotation: Dict[str, Any]) -> Dict[str, Any]:
        """创建空的标注结构"""
        annotation_type = annotation.get('type', '')
        
        # 根据类型创建对应结构
        if 'slots' in annotation:
            return {
                'id': annotation.get('id', ''),
                'type': annotation_type,
                'key': annotation.get('key', ''),
                'slots': []
            }
        elif 'slotsChildren' in annotation:
            return {
                'id': annotation.get('id', ''),
                'type': annotation_type,
                'key': annotation.get('key', ''),
                'slotsChildren': []
            }
        elif 'childrenOnly' in annotation:
            return {
                'id': annotation.get('id', ''),
                'type': annotation_type,
                'key': annotation.get('key', ''),
                'childrenOnly': []
            }
        else:
            return {
                'id': annotation.get('id', ''),
                'type': annotation_type,
                'key': annotation.get('key', ''),
                'label': annotation.get('label', ''),
                'color': annotation.get('color', '')
            }
    
    def split_frames(self, project_path: str) -> bool:
        """拆帧主函数"""
        if not os.path.exists(project_path):
            print(f"项目路径不存在: {project_path}")
            return False
        
        # 查找所有JSON文件
        json_files = []
        for root, dirs, files in os.walk(project_path):
            for file in files:
                if file.endswith('.json') and not file.startswith('.'):
                    json_files.append(os.path.join(root, file))
        
        if not json_files:
            print(f"未找到JSON文件: {project_path}")
            return False
        
        print(f"找到 {len(json_files)} 个JSON文件，开始拆帧...")
        
        # 处理每个文件
        success_count = 0
        for json_file in tqdm(json_files, desc="拆帧进度"):
            if self.process_file(json_file):
                success_count += 1
        
        print(f"拆帧完成！成功处理 {success_count}/{len(json_files)} 个文件")
        return success_count > 0


def to_split(project_path: str) -> bool:
    """拆帧的简化接口"""
    splitter = FrameSplitter()
    return splitter.split_frames(project_path)


def to_split_new(project_path: str) -> bool:
    """新的拆帧接口（兼容旧版本）"""
    return to_split(project_path)