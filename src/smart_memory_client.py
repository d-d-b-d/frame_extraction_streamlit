"""
智能内存版Rosetta客户端
支持自动故障转移，优先使用标准接口，失败后切换到大文件接口
"""

import io
import zipfile
import json
import os
import requests
import random
import time
from typing import Dict, Any, Optional, List
from requests.exceptions import RequestException, Timeout, ConnectionError

# 移除硬编码的文件路径导入，使用更灵活的方式
# 导入原始的两个客户端类
import sys
import importlib.util

# 动态导入标准接口模块 - 使用相对路径和错误处理
def import_rosetta_modules():
    """尝试导入Rosetta相关模块，失败时创建模拟类"""
    global get_rosetta_json, get_rosetta_json_big_backdoor
    
    try:
        # 尝试多种路径导入标准接口
        possible_paths = [
            "/Users/Apple/task/integrate/get_rosetta_json.py",
            "../get_rosetta_json.py", 
            "./get_rosetta_json.py",
            os.path.join(os.path.dirname(__file__), '../../get_rosetta_json.py'),
            os.path.join(os.path.dirname(__file__), '../get_rosetta_json.py')
        ]
        
        get_rosetta_json = None
        for path in possible_paths:
            if os.path.exists(path):
                try:
                    spec = importlib.util.spec_from_file_location("get_rosetta_json", path)
                    get_rosetta_json = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(get_rosetta_json)
                    print(f"✅ 成功从 {path} 导入标准接口模块")
                    break
                except Exception as e:
                    print(f"⚠️  从 {path} 导入失败: {e}")
        
        if get_rosetta_json is None:
            # 创建模拟模块
            class MockGetRosData:
                def __init__(self, **kwargs):
                    self.project_id = kwargs.get('project_id')
                    self.pool_id = kwargs.get('pool_id')
                    self.save_path = kwargs.get('save_path')
                    self._type = kwargs.get('_type', 1)
                    self.is_check_pool = kwargs.get('is_check_pool', False)
                    self.use_dev = False
                    self.get_url = 'https://server.rosettalab.top/rosetta-service/project/doneTask/export'
                    self.req_data = {"projectId": self.project_id, "poolId": self.pool_id, "type": self._type}
                
                def get_headers(self):
                    return {"Content-Type": "application/json"}
            
            get_rosetta_json = type('MockModule', (), {'GetRosData': MockGetRosData})()
            print("⚠️  使用模拟标准接口模块")
            
    except Exception as e:
        print(f"❌ 导入标准接口模块时出错: {e}")
        get_rosetta_json = None

    try:
        # 尝试多种路径导入大文件接口
        possible_paths_big = [
            "/Users/Apple/task/integrate/get_rosetta_json_big_backdoor.py",
            "../get_rosetta_json_big_backdoor.py",
            "./get_rosetta_json_big_backdoor.py", 
            os.path.join(os.path.dirname(__file__), '../../get_rosetta_json_big_backdoor.py'),
            os.path.join(os.path.dirname(__file__), '../get_rosetta_json_big_backdoor.py')
        ]
        
        get_rosetta_json_big_backdoor = None
        for path in possible_paths_big:
            if os.path.exists(path):
                try:
                    spec = importlib.util.spec_from_file_location("get_rosetta_json_big_backdoor", path)
                    get_rosetta_json_big_backdoor = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(get_rosetta_json_big_backdoor)
                    print(f"✅ 成功从 {path} 导入大文件接口模块")
                    break
                except Exception as e:
                    print(f"⚠️  从 {path} 导入失败: {e}")
        
        if get_rosetta_json_big_backdoor is None:
            # 创建模拟模块
            class MockGetRosDataBig:
                def __init__(self, **kwargs):
                    self.project_id = kwargs.get('project_id')
                    self.pool_id = kwargs.get('pool_id')
                    self.save_path = kwargs.get('save_path')
                    self._type = kwargs.get('_type', 1)
                    self.is_check_pool = kwargs.get('is_check_pool', False)
                    self.use_dev = False
                    self.get_url = 'https://server.rosettalab.top/rosetta-service/project/doneTask/export/oss'
                    self.req_data = {"projectId": self.project_id, "poolId": self.pool_id, "type": self._type}
                
                def get_headers(self):
                    return {"Content-Type": "application/json"}
            
            get_rosetta_json_big_backdoor = type('MockModule', (), {'GetRosData': MockGetRosDataBig})()
            print("⚠️  使用模拟大文件接口模块")
            
    except Exception as e:
        print(f"❌ 导入大文件接口模块时出错: {e}")
        get_rosetta_json_big_backdoor = None

# 执行导入
import_rosetta_modules()

import importlib.util
import sys
import os
import requests
import zipfile
import io
import json
from typing import Dict, Optional
from requests.exceptions import RequestException, Timeout, ConnectionError

class SmartMemoryRosettaClient:
    """智能内存版Rosetta数据客户端 - 支持自动故障转移"""
    
    def __init__(self, project_id, pool_id: list, _type=1, 
                 is_check_pool=False, use_dev=False, username=None, password=None):
        """
        Args:
            project_id: 项目ID
            pool_id: 池子ID列表
            _type: 下载类型，0为平台导出，1为任务导出
            is_check_pool: 是否检查池子状态
            use_dev: 是否使用开发环境
            username: 用户名
            password: 密码
        """
        self.project_id = project_id
        self.pool_id = pool_id
        self._type = _type
        self.is_check_pool = is_check_pool
        self.use_dev = use_dev
        self.username = username
        self.password = password
        
        # 初始化两个客户端
        self.standard_client = None
        self.bigfile_client = None
        self._init_clients()
    
    def _init_clients(self):
        """初始化两个客户端实例"""
        try:
            self.standard_client = get_rosetta_json.GetRosData(
                project_id=self.project_id,
                pool_id=self.pool_id,
                save_path='/tmp',  # 临时路径，实际不会用到
                _type=self._type,
                is_check_pool=self.is_check_pool
            )
            print("✅ 标准客户端初始化成功")
        except Exception as e:
            print(f"⚠️  标准客户端初始化失败: {str(e)}")
            self.standard_client = None
        
        try:
            self.bigfile_client = get_rosetta_json_big_backdoor.GetRosData(
                project_id=self.project_id,
                pool_id=self.pool_id,
                save_path='/tmp',  # 临时路径，实际不会用到
                _type=self._type,
                is_check_pool=self.is_check_pool
            )
            print("✅ 大文件客户端初始化成功")
        except Exception as e:
            print(f"⚠️  大文件客户端初始化失败: {str(e)}")
            self.bigfile_client = None
    
    def smart_download(self) -> bytes:
        """智能下载，自动选择最优接口
        
        Returns:
            bytes: ZIP文件的二进制数据
        """
        # 首先尝试标准接口
        if self.standard_client:
            try:
                print("🚀 尝试标准下载接口...")
                
                # 获取数据（不保存到文件）
                response = requests.post(
                    self.standard_client.get_url,
                    json=self.standard_client.req_data,
                    headers=self.standard_client.get_headers(),
                    timeout=30  # 添加超时时间
                )
                
                print(f"标准接口响应状态码: {response.status_code}")
                print(f"标准接口响应大小: {len(response.content)} bytes")
                
                # 处理504超时错误
                if response.status_code == 504:
                    print("⚠️  标准接口超时(504)，将自动切换到大文件接口")
                elif response.status_code == 200 and len(response.content) > 160:
                    # 检查是否为空ZIP
                    if not self._is_zip_data_empty(response.content):
                        print("✅ 标准接口下载成功")
                        return response.content
                    else:
                        print("⚠️  标准接口返回空ZIP，尝试大文件接口")
                else:
                    print(f"⚠️  标准接口响应异常，状态码: {response.status_code}")
                    
            except (Timeout, ConnectionError) as e:
                print(f"⚠️  标准接口连接超时或失败: {str(e)}，将切换到大文件接口")
            except RequestException as e:
                print(f"❌ 标准接口请求失败: {str(e)}")
            except Exception as e:
                print(f"❌ 标准接口其他错误: {str(e)}")
        else:
            print("⚠️  标准客户端不可用，直接尝试大文件接口")
        
        # 标准接口失败，尝试大文件接口
        if self.bigfile_client:
            try:
                print("🔄 切换到大文件接口...")
                
                # 获取OSS下载信息
                response = requests.post(
                    self.bigfile_client.get_url,
                    json=self.bigfile_client.req_data,
                    headers=self.bigfile_client.get_headers(),
                    timeout=60  # 大文件接口可能需要更长时间
                )
                
                print(f"大文件接口响应状态码: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data and len(data['data']) > 0:
                        oss_file_name = data['data'][0]['zipFileName']
                        print(f"获取到OSS文件: {oss_file_name}")
                        
                        # 下载OSS文件到内存
                        zip_data = self._download_oss_file_to_memory(oss_file_name)
                        if zip_data and not self._is_zip_data_empty(zip_data):
                            print("✅ 大文件接口下载成功")
                            return zip_data
                        else:
                            print("⚠️  大文件接口返回空数据")
                    else:
                        print("⚠️  大文件接口无数据返回")
                else:
                    print(f"⚠️  大文件接口响应异常，状态码: {response.status_code}")
                    
            except Exception as e:
                print(f"❌ 大文件接口也失败: {str(e)}")
        else:
            print("⚠️  大文件客户端不可用")
        
        # 所有接口都失败
        raise Exception("所有下载接口都失败了，请检查项目ID、池子ID和网络连接")
    
    def _download_oss_file_to_memory(self, oss_file_name: str) -> bytes:
        """下载OSS文件到内存
        
        Args:
            oss_file_name: OSS文件路径
            
        Returns:
            bytes: 文件内容
        """
        try:
            # 导入OSS相关库（延迟导入，避免不必要的依赖）
            import oss2
            
            # OSS配置（使用与大文件客户端相同的配置）
            config_path = '/Users/Apple/Documents/work/data/oss_config.json'
            with open(config_path, 'r') as f:
                oss_config = json.load(f)
            
            auth = oss2.Auth(oss_config['access_key'], oss_config['secret_key'])
            bucket_name = 'rosetta-data'
            end_point = 'https://oss-cn-beijing.aliyuncs.com'
            bucket = oss2.Bucket(auth, end_point, bucket_name)
            
            # 处理OSS文件路径
            if oss_file_name.startswith('oss://rosetta-data/'):
                oss_file_name = oss_file_name.lstrip('oss://rosetta-data/')
            
            # 下载到内存
            object_stream = bucket.get_object(oss_file_name)
            file_content = object_stream.read()
            
            print(f"OSS文件下载完成，大小: {len(file_content)} bytes")
            return file_content
            
        except Exception as e:
            print(f"OSS文件下载失败: {str(e)}")
            return None
    
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
        """获取项目数据到内存（智能下载并解压）
        
        Returns:
            Dict[str, bytes]: 文件路径到文件内容的映射
        """
        print("开始智能下载数据到内存...")
        
        # 添加重试机制
        max_retries = 2
        for attempt in range(max_retries):
            try:
                zip_data = self.smart_download()
                print("数据下载完成，开始解压到内存...")
                
                files = self.extract_zip_to_memory(zip_data)
                print(f"数据解压完成，共 {len(files)} 个文件")
                
                return files
                
            except Exception as e:
                print(f"第 {attempt + 1} 次尝试失败: {str(e)}")
                if attempt < max_retries - 1:
                    print(f"等待 {2 ** attempt} 秒后重试...")
                    import time
                    time.sleep(2 ** attempt)  # 指数退避
                else:
                    print("所有重试都失败了")
                    raise
        
        raise Exception("下载失败，已达到最大重试次数")