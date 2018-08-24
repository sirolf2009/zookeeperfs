package com.sirolf2009.zookeeperfs

import java.nio.ByteBuffer
import java.util.List
import org.apache.zookeeper.ZooKeeper
import org.jboss.shrinkwrap.api.nio.file.SeekableInMemoryByteChannel
import java.io.IOException

class ZookeeperSeekableByteChannel extends SeekableInMemoryByteChannel {
	
	val ZooKeeper zookeeper
	val String path
	
	new(ZooKeeper zookeeper, String path, List<Byte> content) {
		this.zookeeper = zookeeper
		this.path = path
		write(ByteBuffer.wrap(content))
	}
	
	override close() throws IOException {
		zookeeper.setData(path, getContents(), -1)
		super.close()
	}

}
