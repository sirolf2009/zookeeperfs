package com.sirolf2009.zookeeperfs

import java.net.URI
import java.nio.file.Paths
import org.junit.Test
import java.nio.file.Files
import org.junit.Assert

class ZooKeeperFileSystemProviderTest {
	
	@Test
	def void testCreatePath() {
		Assert.assertEquals("//", ((Paths.get(URI.create("zookeeper://localhost:2181/")) as ZooKeeperPath).getPath()))
		Assert.assertEquals("/playground/", ((Paths.get(URI.create("zookeeper://localhost:2181/playground")) as ZooKeeperPath).getPath()))
	}
	
	@Test
	def void testDirectoryStream() {
		val path = Paths.get(URI.create("zookeeper://localhost:2181/"))
		val stream = Files.newDirectoryStream(path)
		stream.forEach[
			if(Files.isRegularFile(it)) {
				println("File: "+toString())
				println(new String(Files.readAllBytes(it)))
			} else {
				println(toString()+"/")
			}
		]
		stream.close()
	}
	
	@Test
	def void getZooKeeperPath() {
		Assert.assertEquals("/", ZooKeeperFileSystemProvider.getZooKeeperPath("/"))
		Assert.assertEquals("/", ZooKeeperFileSystemProvider.getZooKeeperPath("//"))
		Assert.assertEquals("/cluster", ZooKeeperFileSystemProvider.getZooKeeperPath("//cluster"))
	}
	
	@Test
	def void ensureNoTrailingSlash() {
		Assert.assertEquals("/playground", ZooKeeperFileSystemProvider.ensureNoTrailingSlash("/playground"))
		Assert.assertEquals("/playground", ZooKeeperFileSystemProvider.ensureNoTrailingSlash("/playground/"))
		Assert.assertEquals("/playground", ZooKeeperFileSystemProvider.ensureNoTrailingSlash("/playground//"))
	}
	
}