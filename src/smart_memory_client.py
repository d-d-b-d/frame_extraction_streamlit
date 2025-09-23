"""
智能内存版Rosetta客户端
支持自动故障转移，优先使用标准接口，失败后切换到大文件接口
集成了标准接口和大文件接口的功能
"""

import io
import zipfile
import json
import os
import requests
import random
import time
import shutil
from typing import Dict, Any, Optional, List

# OSS支持（大文件接口需要）
try:
    import oss2
    OSS_AVAILABLE = True
except ImportError:
    OSS_AVAILABLE = False
    print("⚠️  OSS库不可用，大文件接口功能受限")


class Auth:
    """认证类"""
    
    def __init__(self, use_dev=False):
        """
        Args:
            use_dev: 是否使用开发环境
        """
        if use_dev:
            self.login_url = 'https://dev-server.rosettalab.top/rosetta-service/user/login'
        else:
            self.login_url = 'https://server.rosettalab.top/rosetta-service/user/login'

    def get_authorize(self, username=None, password=None):
        """获取认证token
        
        Args:
            username: 用户名，如果为None需要用户输入
            password: 密码，如果为None需要用户输入
        """
        # 优先使用传入的用户名密码，其次尝试从配置文件读取，最后提示用户输入
        username = username or os.getenv('ROSETTA_USERNAME')
        password = password or os.getenv('ROSETTA_PASSWORD')
        
        if not username or not password:
            raise ValueError("请提供Rosetta认证信息：\n"
                           "1. 在config.yaml中配置rosetta.username和rosetta.password\n"
                           "2. 或者设置环境变量ROSETTA_USERNAME和ROSETTA_PASSWORD\n"
                           "3. 或者在运行时通过命令行参数提供")

        data = {
            "username": username,
            "password": password
        }
        data = json.dumps(data, separators=(',', ':'))
        
        response = requests.post(self.login_url, headers={
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
        }, data=data)
        
        response_data = response.json()
        
        if 'data' not in response_data or 'tokenValue' not in response_data['data']:
            error_msg = "认证失败"
            if 'message' in response_data:
                error_msg += f": {response_data['message']}"
            elif 'msg' in response_data:
                error_msg += f": {response_data['msg']}"
            else:
                error_msg += f": {response_data}"
            raise ValueError(error_msg)
        
        return response_data['data']['tokenValue']


class StandardClient(Auth):
    """标准接口客户端"""
    
    def __init__(self, project_id, pool_id: list, save_path='./', _type=1, 
                 is_check_pool=False, use_dev=False, username=None, password=None):
        """
        Args:
            project_id: 项目ID
            pool_id: 池子ID列表
            save_path: 保存路径
            _type: 下载类型，0为平台导出，1为任务导出
            is_check_pool: 是否检查池子状态
            use_dev: 是否使用开发环境
            username: 用户名
            password: 密码
        """
        super().__init__(use_dev)
        
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
            
        self.save_path = save_path
        self.save_file = os.path.join(save_path, f"{self.project_id}.zip")
        os.makedirs(save_path, exist_ok=True)

    def _generate_session_id(self):
        """生成会话ID"""
        n = [''] * 20
        o = list(str(int(time.time() * 1000))[::-1])
        for i in range(20):
            e = random.randint(0, 35)
            t = str(e if e < 26 else chr(e + 87))
            n[i] = t if e % 3 else t.upper()
        for i in range(8):
            n.insert(3 * i + 2, o[i])
        return ''.join(n[::-1])

    def get_headers(self):
        """获取请求头"""
        return {
            "authority": "server.rosettalab.top",
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN",
            "authorize": self.get_authorize(self.username, self.password),
            "content-type": "application/json",
            "eagleeye-pappname": "ez9nyt1o1w@97036bb7d21afb4",
            "eagleeye-sessionid": self._generate_session_id(),
            "eagleeye-traceid": "bdc52817169027484816010061afb4",
            "origin": "https://rosettalab.top",
            "referer": "https://rosettalab.top/",
            "sec-ch-ua": "\"Google Chrome\";v=\"111\", \"Not(A:Brand\";v=\"8\", \"Chromium\";v=\"111\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"macOS\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
        }


class BigFileClient(Auth):
    """大文件接口客户端"""
    
    def __init__(self, project_id, pool_id: list, save_path='./', _type=0, 
                 is_check_pool=False, use_dev=False, username=None, password=None):
        """
        Args:
            project_id: 项目ID
            pool_id: 池子ID列表
            save_path: 保存路径
            _type: 下载类型，0为平台导出，1为任务导出
            is_check_pool: 是否检查池子状态
            use_dev: 是否使用开发环境
            username: 用户名
            password: 密码
        """
        super().__init__(use_dev)
        
        # 大文件接口使用特殊的URL
        self.get_url = 'https://server.rosettalab.top/rosetta-service/backDoor/queryProjectExportLog'
        
        self.project_id = project_id
        self.pool_id = pool_id
        self.username = username
        self.password = password
        
        # 大文件接口需要特殊的请求数据格式
        self.req_data = {"projectId": project_id, "poolId": str(pool_id[0]), "type": _type}
        if is_check_pool:
            self.req_data["poolType"] = 3
            
        self.save_path = save_path
        self.save_file = os.path.join(save_path, f"{self.project_id}.zip")
        os.makedirs(save_path, exist_ok=True)

    def get_headers(self):
        """获取大文件接口的请求头"""
        def generate_string():
            n = [''] * 20
            o = list(str(int(time.time() * 1000))[::-1])
            for i in range(20):
                e = random.randint(0, 35)
                t = str(e if e < 26 else chr(e + 87))
                n[i] = t if e % 3 else t.upper()
            for i in range(8):
                n.insert(3 * i + 2, o[i])
            return ''.join(n[::-1])

        return {
            'userId': '170152',
            'teamId': '0',
            'Authorize': self.get_authorize(self.username, self.password),
            'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Cache-Control': 'no-cache',
            'Host': 'server.rosettalab.top',
            'Connection': 'keep-alive',
        }


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
            self.standard_client = StandardClient(
                project_id=self.project_id,
                pool_id=self.pool_id,
                save_path='/tmp',  # 临时路径，实际不会用到
                _type=self._type,
                is_check_pool=self.is_check_pool,
                use_dev=self.use_dev,
                username=self.username,
                password=self.password
            )
            print("✅ 标准客户端初始化成功")
        except Exception as e:
            print(f"⚠️  标准客户端初始化失败: {str(e)}")
            self.standard_client = None
        
        try:
            self.bigfile_client = BigFileClient(
                project_id=self.project_id,
                pool_id=self.pool_id,
                save_path='/tmp',  # 临时路径，实际不会用到
                _type=self._type,
                is_check_pool=self.is_check_pool,
                use_dev=self.use_dev,
                username=self.username,
                password=self.password
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
                    timeout=30  # 添加超时设置
                )
                
                print(f"标准接口响应状态码: {response.status_code}")
                print(f"标准接口响应大小: {len(response.content)} bytes")
                
                # 处理504网关超时错误
                if response.status_code == 504:
                    print("⚠️  标准接口504网关超时，立即切换到大文件接口")
                elif response.status_code == 200 and len(response.content) > 160:
                    # 检查是否为空ZIP
                    if not self._is_zip_data_empty(response.content):
                        print("✅ 标准接口下载成功")
                        return response.content
                    else:
                        print("⚠️  标准接口返回空ZIP，尝试大文件接口")
                else:
                    print(f"⚠️  标准接口响应异常，状态码: {response.status_code}")
                    
            except requests.exceptions.Timeout:
                print("⚠️  标准接口请求超时，切换到大文件接口")
            except requests.exceptions.RequestException as e:
                print(f"❌ 标准接口网络错误: {str(e)}")
            except Exception as e:
                print(f"❌ 标准接口失败: {str(e)}")
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
                
                if response.status_code == 504:
                    print("⚠️  大文件接口也返回504网关超时")
                elif response.status_code == 200:
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
                    
            except requests.exceptions.Timeout:
                print("⚠️  大文件接口请求超时")
            except requests.exceptions.RequestException as e:
                print(f"❌ 大文件接口网络错误: {str(e)}")
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
        if not OSS_AVAILABLE:
            print("❌ OSS库不可用，无法下载大文件")
            return None
            
        try:
            # OSS配置（使用与大文件客户端相同的配置）
            config_path = '/Users/Apple/Documents/work/data/oss_config.json'
            if not os.path.exists(config_path):
                print(f"❌ OSS配置文件不存在: {config_path}")
                return None
                
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
        zip_data = self.smart_download()
        print("数据下载完成，开始解压到内存...")
        
        files = self.extract_zip_to_memory(zip_data)
        print(f"数据解压完成，共 {len(files)} 个文件")
        
        return files