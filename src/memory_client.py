"""
内存版Rosetta客户端
支持在内存中处理数据，不依赖本地文件系统
"""

import io
import zipfile
import json
import os
from typing import Dict, Any, Optional, List
from rosetta_client import GetRosData, Auth


class MemoryRosettaClient(GetRosData):
    """内存版Rosetta数据客户端"""
    
    def __init__(self, project_id, pool_id: list, _type=1, 
                 is_check_pool=False, use_dev=False, username=None, password=None):
        """
        Args:
            project_id: 项目ID
            pool_id: 池子ID列表
            _type: 导出格式类型，0为平台格式导出，1为任务格式导出
            is_check_pool: 是否检查池子状态
            use_dev: 是否使用开发环境
            username: 用户名
            password: 密码
        """
        # 直接初始化父类的属性，避免文件系统操作
        Auth.__init__(self, use_dev)
        
        if use_dev:
            self.get_url = 'https://dev-server.rosettalab.top/rosetta-service/project/doneTask/export'
        else:
            self.get_url = 'https://server.rosettalab.top/rosetta-service/project/doneTask/export'

        self.project_id = project_id
        self.pool_id = pool_id
        self.username = username
        self.password = password
        
        self.req_data = {"projectId": project_id, "poolId": pool_id, "type": _type}
        if is_check_pool:
            self.req_data["poolType"] = 3
            
        # 不设置文件系统相关属性
        self.save_path = None
        self.save_file = None
    
    def get_data_to_memory(self) -> bytes:
        """下载数据到内存
        
        Returns:
            bytes: ZIP文件的二进制数据
        """
        import requests
        
        resq = requests.post(self.get_url, json=self.req_data, headers=self._get_headers())
        
        print(f"API响应状态码: {resq.status_code}")
        print(f"API响应大小: {len(resq.content)} bytes")
        
        if resq.status_code != 200:
            error_msg = f"API请求失败，状态码: {resq.status_code}"
            try:
                error_json = resq.json()
                if 'message' in error_json:
                    error_msg += f", 错误信息: {error_json['message']}, 请确认配置信息中check_pool状态是否与当前任务匹配"
            except:
                error_msg += f", 响应: {resq.text[:200]}"
            raise ValueError(error_msg)
        
        # 检查是否为空ZIP
        if self._is_zip_data_empty(resq.content):
            raise ValueError("下载的数据为空或格式错误，请检查项目ID和池子ID是否正确")
        
        return resq.content
    
    def _is_zip_data_empty(self, zip_data: bytes) -> bool:
        """检查ZIP数据是否为空"""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_data)) as zip_file:
                return len(zip_file.namelist()) == 0
        except zipfile.BadZipFile:
            return True
    
    def extract_zip_to_memory(self, zip_data: bytes) -> Dict[str, bytes]:
        """将ZIP数据解压到内存
        
        Args:
            zip_data: ZIP文件的二进制数据
            
        Returns:
            Dict[str, bytes]: 文件路径到文件内容的映射
        """
        result_files = {}
        
        try:
            with zipfile.ZipFile(io.BytesIO(zip_data)) as zip_file:
                for file_info in zip_file.filelist:
                    if not file_info.is_dir():
                        file_content = zip_file.read(file_info.filename)
                        result_files[file_info.filename] = file_content
        except Exception as e:
            raise ValueError(f"解压ZIP数据失败: {str(e)}")
        
        return result_files
    
    def get_project_data_to_memory(self) -> Dict[str, bytes]:
        """获取项目数据到内存（下载并解压）
        
        Returns:
            Dict[str, bytes]: 文件路径到文件内容的映射
        """
        print("开始下载数据到内存...")
        zip_data = self.get_data_to_memory()
        print("数据下载完成，开始解压到内存...")
        
        files = self.extract_zip_to_memory(zip_data)
        print(f"数据解压完成，共 {len(files)} 个文件")
        
        return files


class MemoryFrameExtractor:
    """内存版帧提取器"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Args:
            config: 配置字典
        """
        self.config = config
    
    def extract_frames_from_memory(self, files_dict: Dict[str, bytes]) -> Dict[str, bytes]:
        """从内存文件中提取帧
        
        Args:
            files_dict: 文件路径到文件内容的映射
            
        Returns:
            Dict[str, bytes]: 包含提取结果的新文件字典
        """
        if not self.config['frame_extraction']['enabled']:
            return files_dict
        
        # 实现与原始frame_splitter完全相同的逻辑，但在内存中
        return self._split_frames_in_memory(files_dict)
    
    def _split_frames_in_memory(self, files_dict: Dict[str, bytes]) -> Dict[str, bytes]:
        """在内存中执行拆帧操作，保持与原始frame_splitter完全相同的文件结构
        
        Args:
            files_dict: 文件路径到文件内容的映射
            
        Returns:
            Dict[str, bytes]: 包含拆帧结果的新文件字典
        """
        result_files = {}
        
        # 查找所有JSON文件，按文件名字典序排序（与os.walk保持一致）
        json_files = []
        for file_path in files_dict.keys():
            if file_path.endswith('.json') and not os.path.basename(file_path).startswith('.'):
                json_files.append(file_path)
        
        # 按文件路径排序，确保处理顺序与原始版本一致
        json_files.sort()
        
        if not json_files:
            print("未找到JSON文件")
            return files_dict
        
        print(f"找到 {len(json_files)} 个JSON文件，开始拆帧...")
        
        # 处理每个文件
        success_count = 0
        for json_file in json_files:
            try:
                # 加载JSON数据
                json_data = json.loads(files_dict[json_file].decode('utf-8'))
                
                # 检查是否是序列类型
                record = json_data.get('taskParams', {}).get('record', {})
                attachment_type = record.get('attachmentType', '')
                
                if attachment_type not in ["IMAGE_SEQUENCE", "IMAGE_SET_SEQUENCE", "POINTCLOUD_SEQUENCE", "POINTCLOUD_SET_SEQUENCE"]:
                    print(f"跳过非序列文件: {json_file}")
                    # 保留非序列文件（与原始版本一致，非序列文件不会被删除）
                    result_files[json_file] = files_dict[json_file]
                    success_count += 1
                    continue
                
                # 提取基本信息
                project_id = json_data.get('projectId', 0)
                dataset_id = json_data.get('datasetId', 0)
                pool_id = json_data.get('poolId', 0)
                task_id = json_data.get('taskId', 0)
                status = json_data.get('status', 0)
                
                # 获取附件信息
                attachment = record.get('attachment', [])
                attachment_length = len(attachment)
                metadata = record.get('metadata', {})
                operators = json_data.get('taskParams', {}).get('operators', [])
                
                # 获取结果信息
                result_annotations = json_data.get('result', {}).get('annotations', [])
                result_hints = json_data.get('result', {}).get('hints', [])
                result_metadata = json_data.get('result', {}).get('metadata', {})
                
                if attachment_length == 0:
                    print(f"跳过无附件的文件: {json_file}")
                    # 保留无附件的序列文件（与原始版本一致）
                    result_files[json_file] = files_dict[json_file]
                    success_count += 1
                    continue
                
                # 为每一帧创建新文件
                for frame_number in range(attachment_length):
                    frame_data = self._create_frame_data(
                        project_id, dataset_id, pool_id, task_id, status,
                        attachment_type, attachment, metadata, operators,
                        result_annotations, result_hints, result_metadata,
                        frame_number
                    )
                    
                    # 生成新文件路径（保持与原始frame_splitter相同的结构）
                    new_path = self._frame_name(json_file, frame_number)
                    
                    # 创建task_id目录结构
                    task_dir = os.path.join(os.path.dirname(new_path), str(task_id))
                    final_path = os.path.join(task_dir, os.path.basename(new_path))
                    
                    # 保存帧数据到内存
                    frame_content = json.dumps(frame_data, ensure_ascii=False, indent=2).encode('utf-8')
                    result_files[final_path] = frame_content
                
                # 删除原始文件（与原始frame_splitter保持一致）
                # 不将原始文件加入结果，保持与原始版本相同的行为
                print(f"已处理: {json_file} -> {attachment_length} 帧")
                success_count += 1
                
            except Exception as e:
                print(f"处理文件失败 {json_file}: {str(e)}")
                # 处理失败的文件也保留
                result_files[json_file] = files_dict[json_file]
        
        print(f"拆帧完成！成功处理 {success_count}/{len(json_files)} 个文件")
        
        # 保留所有非JSON文件
        for file_path, file_content in files_dict.items():
            if not file_path.endswith('.json'):
                result_files[file_path] = file_content
        
        # 保持原始的文件处理顺序，不强制排序
        # 文件应该按照处理的先后顺序自然排列
        return result_files
    
    def _frame_name(self, original_path: str, frame_number: int) -> str:
        """生成帧文件名（与原始frame_splitter相同逻辑）"""
        base_name = os.path.basename(original_path)
        name_without_ext = os.path.splitext(base_name)[0]
        dir_name = os.path.dirname(original_path)
        return os.path.join(dir_name, f"{name_without_ext}_{frame_number:06d}.json")
    
    def _create_frame_data(self, project_id: int, dataset_id: int, pool_id: int, 
                          task_id: int, status: int, attachment_type: str,
                          attachment: list, metadata: dict, operators: list,
                          result_annotations: list, result_hints: list,
                          result_metadata: dict, frame_number: int) -> Dict[str, Any]:
        """创建单帧数据（与原始frame_splitter相同逻辑）"""
        
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
        """创建空的标注结构（与原始frame_splitter相同逻辑）"""
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