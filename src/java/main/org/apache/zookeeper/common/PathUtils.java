 /**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.common;


/**
 * Path related utilities
 */    
public class PathUtils {
	
	/** validate the provided znode path string
	 * @param path znode path string
	 * @param isSequential if the path is being created
	 * with a sequential flag
	 * @throws IllegalArgumentException if the path is invalid
	 */
	public static void validatePath(String path, boolean isSequential) 
		throws IllegalArgumentException {
		validatePath(isSequential? path + "1": path);
	}
	
    /**
     * Validate the provided znode path string
     * 验证提供的znode路径字符串
     * @param path znode path string
     * @throws IllegalArgumentException if the path is invalid
     */
    public static void validatePath(String path) throws IllegalArgumentException {
        // 首先检查整个字符串path
        // 1.非null
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }
        // 2.非空
        if (path.length() == 0) {
            throw new IllegalArgumentException("Path length must be > 0");
        }
        // 3.必须/开头
        if (path.charAt(0) != '/') {
            throw new IllegalArgumentException(
                         "Path must start with / character");
        }
        // 4.根路径,直接返回检查完毕
        if (path.length() == 1) { // done checking - it's the root
            return;
        }
        // 5.不以/结尾
        if (path.charAt(path.length() - 1) == '/') {
            throw new IllegalArgumentException(
                         "Path must not end with / character");
        }
        // 最后检查字符串中的每个字符
        String reason = null;
        char lastc = '/';
        char chars[] = path.toCharArray();
        char c;

        for (int i = 1; i < chars.length; lastc = chars[i], i++) {
            c = chars[i];
            // 6.不能为null
            if (c == 0) {
                reason = "null character not allowed @" + i;
                break;
            // 7.
            } else if (c == '/' && lastc == '/') {
                reason = "empty node name specified @" + i;
                break;
            // 8.不允许相对路径
            } else if (c == '.' && lastc == '.') {
                if (chars[i-2] == '/' &&
                        ((i + 1 == chars.length)
                                || chars[i+1] == '/')) {
                    reason = "relative paths not allowed @" + i;
                    break;
                }
            // 9.不允许相对路径
            } else if (c == '.') {
                if (chars[i-1] == '/' &&
                        ((i + 1 == chars.length)
                                || chars[i+1] == '/')) {
                    reason = "relative paths not allowed @" + i;
                    break;
                }
                // 10.特殊符号
            } else if (c > '\u0000' && c < '\u001f'
                    || c > '\u007f' && c < '\u009F'
                    || c > '\ud800' && c < '\uf8ff'
                    || c > '\ufff0' && c < '\uffff') {
                reason = "invalid character @" + i;
                break;
            }
        }

        if (reason != null) {
            throw new IllegalArgumentException(
                    "Invalid path string \"" + path + "\" caused by " + reason);
        }
    }
}
