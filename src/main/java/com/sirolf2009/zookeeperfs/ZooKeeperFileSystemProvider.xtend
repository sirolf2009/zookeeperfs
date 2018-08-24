package com.sirolf2009.zookeeperfs

import java.io.IOException
import java.net.URI
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream.Filter
import java.nio.file.FileSystemAlreadyExistsException
import java.nio.file.LinkOption
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.ProviderMismatchException
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.spi.FileSystemProvider
import java.util.HashMap
import java.util.Map
import java.util.Set
import org.apache.zookeeper.CreateMode

class ZooKeeperFileSystemProvider extends FileSystemProvider {
	
	val filesystemsCache = new HashMap<URI, ZooKeeperFileSystem>
	
	override getScheme() {
		return "zookeeper"
	}
	
	override newFileSystem(URI uri, Map<String, ?> env) throws IOException {
		synchronized (filesystemsCache) {
			if(filesystemsCache.containsKey(uri)) {
				throw new FileSystemAlreadyExistsException("A file system already exists for "+uri)
			}
			filesystemsCache.put(uri, new ZooKeeperFileSystem(this, uri))
		}
	}
	
	override checkAccess(Path path, AccessMode... modes) throws IOException {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}
	
	override copy(Path source, Path target, CopyOption... options) throws IOException {
		copy(source.checkPath(), target.checkPath(), options)
	}
	
	def copy(ZooKeeperPath source, ZooKeeperPath target, CopyOption... options) throws IOException {
		val data = source.getZooKeeper().getData(source.toString(), false, null)
		target.getZooKeeper().setData(target.toString(), data, -1)
		if(options.contains(StandardCopyOption.COPY_ATTRIBUTES)) {
			target.getZooKeeper().setACL(target.toString(), source.getZooKeeper().getACL(source.toString(), null), -1)
		}
	}
	
	override createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
		createDirectory(dir.checkPath(), attrs)
	}
	
	def createDirectory(ZooKeeperPath dir, FileAttribute<?>... attrs) throws IOException {
		dir.getZooKeeper().create(dir.toString(), null, null, CreateMode.PERSISTENT)
	}
	
	override delete(Path path) throws IOException {
		delete(path.checkPath())
	}
	
	def delete(ZooKeeperPath path) throws IOException {
		path.getZooKeeper().delete(path.toString(), -1)
	}
	
	override <V extends FileAttributeView> getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}
	
	override getFileStore(Path path) throws IOException {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}
	
	override getFileSystem(URI uri) {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}
	
	override getPath(URI uri) {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}
	
	override isHidden(Path path) throws IOException {
		return false
	}
	
	override isSameFile(Path path, Path path2) throws IOException {
		path.checkPath().toString().equals(path2.checkPath().toString())
	}
	
	override move(Path source, Path target, CopyOption... options) throws IOException {
		move(source.checkPath(), target.checkPath(), options)
	}
	
	def move(ZooKeeperPath source, ZooKeeperPath target, CopyOption... options) throws IOException {
		copy(source, target, options)
		delete(source)
	}
	
	override newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
		return newByteChannel(path.checkPath(), options, attrs)
	}
	
	def newByteChannel(ZooKeeperPath path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
		val data = path.getZooKeeper().getData(path.toString(), false, null)
		return new ZookeeperSeekableByteChannel(path.getZooKeeper(), path.toString(), data)
	}
	
	override newDirectoryStream(Path dir, Filter<? super Path> filter) throws IOException {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}
	
	override <A extends BasicFileAttributes> readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}
	
	override readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}
	
	override setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}
	
	def static checkPath(Path path) {
		if(path === null) {
			throw new NullPointerException()
		} else if(!(path instanceof ZooKeeperPath)) {
			throw new ProviderMismatchException()
		} else {
			return path as ZooKeeperPath
		}
	}
	
}