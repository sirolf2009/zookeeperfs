package com.sirolf2009.zookeeperfs

import java.nio.file.attribute.BasicFileAttributes
import org.eclipse.xtend.lib.annotations.Data
import java.nio.file.attribute.FileTime

@Data class ZooKeeperFileAttributes implements BasicFileAttributes {
	
	val long cTime
	val long mTime
	val long dataLength
	val long numChildren
	
	override creationTime() {
		return FileTime.fromMillis(cTime)
	}
	
	override fileKey() {
		return null
	}
	
	override isDirectory() {
		return numChildren > 0
	}
	
	override isOther() {
		return false
	}
	
	override isRegularFile() {
		return !isDirectory()
	}
	
	override isSymbolicLink() {
		return false
	}
	
	override lastAccessTime() {
		return lastModifiedTime()
	}
	
	override lastModifiedTime() {
		return FileTime.fromMillis(mTime)
	}
	
	override size() {
		return dataLength
	}
	
}