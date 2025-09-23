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

# 导入本地客户端模块
from rosetta_client import GetRosData as StandardClient
from rosetta_bigfile_client import RosettaBigFileClient as BigFileClient

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
            # 大文件客户端使用_type=0（平台导出模式）
            self.bigfile_client = BigFileClient(
                project_id=self.project_id,
                pool_id=self.pool_id,
                save_path='/tmp',  # 临时路径，实际不会用到
                _type=0,  # 大文件接口使用平台导出模式
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
        print(f"🚀 开始智能下载，项目ID: {self.project_id}, 池子ID: {self.pool_id}")
        
        # 首先尝试标准接口
        if self.standard_client:
            try:
                print("🚀 尝试标准下载接口...")
                
                # 获取数据（不保存到文件）
                response = requests.post(
                    self.standard_client.get_url,
                    json=self.standard_client.req_data,
                    headers=self.standard_client._get_headers(),
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
                    if response.status_code == 200:
                        print(f"响应内容预览: {response.content[:200]}...")
                    
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
                print(f"请求数据: {self.bigfile_client.req_data}")
                
                # 获取OSS下载信息
                response = requests.post(
                    self.bigfile_client.get_url,
                    json=self.bigfile_client.req_data,
                    headers=self.bigfile_client._get_headers(),
                    timeout=60  # 大文件接口可能需要更长时间
                )
                
                print(f"大文件接口响应状态码: {response.status_code}")
                
                if response.status_code == 504:
                    print("⚠️  大文件接口也返回504网关超时")
                elif response.status_code == 200:
                    try:
                        data = response.json()
                        print(f"大文件接口响应数据: {data}")
                        
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
                            print(f"响应数据结构: {data}")
                    except json.JSONDecodeError as e:
                        print(f"❌ 大文件接口响应不是有效的JSON: {str(e)}")
                        print(f"原始响应内容: {response.text[:500]}...")
                else:
                    print(f"⚠️  大文件接口响应异常，状态码: {response.status_code}")
                    print(f"错误响应内容: {response.text[:500]}...")
                    
            except requests.exceptions.Timeout:
                print("⚠️  大文件接口请求超时")
            except requests.exceptions.RequestException as e:
                print(f"❌ 大文件接口网络错误: {str(e)}")
            except Exception as e:
                print(f"❌ 大文件接口也失败: {str(e)}")
                import traceback
                print(f"详细错误信息: {traceback.format_exc()}")
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
            
            # OSS配置 - 从Streamlit Cloud secrets获取
            import streamlit as st
            oss_config = {
                'access_key': st.secrets["oss_credentials"]["access_key"],
                'secret_key': st.secrets["oss_credentials"]["secret_key"]
            }
            print("✅ 成功从Streamlit Cloud secrets获取OSS配置")
            
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
            
        except ImportError:
            print("❌ Streamlit库未安装，无法获取OSS配置")
            return None
        except KeyError as e:
            print(f"❌ OSS配置未在Streamlit Cloud secrets中设置：{str(e)}")
            return None
        except FileNotFoundError:
            print("❌ Streamlit Cloud secrets文件未找到，请确保应用在Streamlit Cloud环境中运行")
            return None
        except Exception as e:
            print(f"❌ OSS文件下载失败: {str(e)}")
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