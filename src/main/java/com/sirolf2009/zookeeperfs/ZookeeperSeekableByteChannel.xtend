package com.sirolf2009.zookeeperfs

import java.nio.ByteBuffer
import java.util.List
import org.apache.zookeeper.ZooKeeper
import org.jboss.shrinkwrap.api.nio.file.SeekableInMemoryByteChannel
import java.io.IOException

class ZookeeperSeekableByteChannel extends SeekableInMemoryByteChannel {
	
	val ZooKeeper zookeeper
	val String path
	val List<Byte> originalContent
	
	new(ZooKeeper zookeeper, String path, List<Byte> originalContent) {
		this.zookeeper = zookeeper
		this.path = path
		this.originalContent = originalContent
		if(originalContent !== null) {
			write(ByteBuffer.wrap(originalContent))
		}
		position(0)
	}
	
	override close() throws IOException {
		if(!originalContent.equals(getContents().toList())) {
			zookeeper.setData(path, getContents(), -1)
		}
		super.close()
	}

}
