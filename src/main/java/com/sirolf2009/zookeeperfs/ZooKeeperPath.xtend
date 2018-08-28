package com.sirolf2009.zookeeperfs

import java.io.IOException
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.WatchEvent.Kind
import java.nio.file.WatchEvent.Modifier
import java.nio.file.WatchService
import java.util.List
import java.nio.file.ProviderMismatchException
import org.eclipse.xtend.lib.annotations.Accessors

@Accessors class ZooKeeperPath implements Path {

	val ZooKeeperFileSystem fileSystem
	val String path
	val List<String> explodedPath

	new(ZooKeeperFileSystem fileSystem, String path) {
		this.fileSystem = fileSystem
		this.path = path
		this.explodedPath = path.split("/")
	}

	override compareTo(Path other) {
		return path.compareTo(other.checkPath().path)
	}

	override endsWith(Path other) {
		return path.endsWith(other.checkPath().path)
	}

	override endsWith(String other) {
		return path.endsWith(other)
	}

	override getFileName() {
		return new ZooKeeperPath(fileSystem, explodedPath.last())
	}

	override getFileSystem() {
		return fileSystem
	}

	override getName(int index) {
		return new ZooKeeperPath(fileSystem, (0 ..< index).map[explodedPath.get(it)].join("/"))
	}

	override getNameCount() {
		return explodedPath.size()
	}

	override getParent() {
		if(explodedPath.size() > 1) {
			return new ZooKeeperPath(fileSystem, (0 ..< getNameCount() - 1).map[explodedPath.get(it)].join("/"))
		} else {
			return null
		}
	}

	override getRoot() {
		return new ZooKeeperPath(fileSystem, "/")
	}

	override isAbsolute() {
		return path.startsWith("/")
	}

	override iterator() {
		(0 ..< getNameCount()).map[getName(it)].iterator()
	}

	override normalize() {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}

	override register(WatchService watcher, Kind<?>... events) throws IOException {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}

	override register(WatchService watcher, Kind<?>[] events, Modifier... modifiers) throws IOException {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}

	override relativize(Path other) {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}

	override resolve(Path other) {
		return resolve(other.checkPath())
	}

	def resolve(ZooKeeperPath other) {
		if(other.isAbsolute()) {
			return other
		}
		return new ZooKeeperPath(fileSystem, if(path.endsWith("/")) path + other.path else path + "/" + other.path)
	}

	override resolve(String other) {
		return resolve(fileSystem.getPath(other))
	}

	override resolveSibling(Path other) {
		val parent = getParent()
		if(parent === null) {
			return other
		} else {
			return parent.resolve(other)
		}
	}

	override resolveSibling(String other) {
		resolveSibling(fileSystem.getPath(other))
	}

	override startsWith(Path other) {
		return path.startsWith(other.checkPath().path)
	}

	override startsWith(String other) {
		return path.startsWith(other)
	}

	override subpath(int beginIndex, int endIndex) {
		return new ZooKeeperPath(fileSystem, explodedPath.subList(beginIndex, endIndex).join("/"))
	}

	override toAbsolutePath() {
		if(isAbsolute()) {
			return this
		}
		return new ZooKeeperPath(fileSystem, "/" + explodedPath.join("/"))
	}

	override toFile() {
		throw new UnsupportedOperationException()
	}

	override toRealPath(LinkOption... options) throws IOException {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}

	override toUri() {
		throw new UnsupportedOperationException("TODO: auto-generated method stub")
	}
	
	override toString() {
		return path
	}
	
	def getZKFileSystem() {
		return fileSystem
	}
	
	def getZooKeeper() {
		return fileSystem.getZooKeeper()
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
