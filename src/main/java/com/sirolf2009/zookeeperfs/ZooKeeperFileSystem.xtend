package com.sirolf2009.zookeeperfs

import java.io.IOException
import java.net.URI
import java.nio.file.FileSystem
import java.util.List
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.ZooKeeper

class ZooKeeperFileSystem extends FileSystem {
	
	val ZooKeeperFileSystemProvider fileSystemProvider
	val ZooKeeper zookeeper
	
	new(ZooKeeperFileSystemProvider fileSystemProvider, URI uri) {
		this.fileSystemProvider = fileSystemProvider
		zookeeper = connect(#['''«uri.getHost()»:«uri.getPort()»'''])
	}
	
	override provider() {
		return fileSystemProvider
	}

	def static connect(List<String> endPoints) {
		val connectionLatch = new CountDownLatch(endPoints.size())
		val zookeeper = new ZooKeeper(endPoints.join(","), 2000, [
			if(getState().equals(KeeperState.SyncConnected)) {
				connectionLatch.countDown()
			}
		])
		if(!connectionLatch.await(1, TimeUnit.MINUTES)) {
			throw new IOException("Failed to connect")
		}
		return zookeeper
	}
	
	override close() throws IOException {
		zookeeper.close()
	}
	
	override getRootDirectories() {
		return #[new ZooKeeperPath(this, "/")]
	}
	
	override getFileStores() {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}
	
	override getPath(String first, String... more) {
		if(more.size() == 0) {
			return new ZooKeeperPath(this, first+"/"+more.join("/"))
		}
	}
	
	override getPathMatcher(String syntaxAndPattern) {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}
	
	override getSeparator() {
		return "/"
	}
	
	override getUserPrincipalLookupService() {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}
	
	override isOpen() {
		zookeeper.getState().isAlive()
	}
	
	override isReadOnly() {
		return false
	}
	
	override newWatchService() throws IOException {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}
	
	override supportedFileAttributeViews() {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}
	
	def getZooKeeper() {
		return zookeeper
	}
	
}