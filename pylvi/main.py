import collections
import errno
import fcntl
import logging
import os
import pickle
import select
import cStringIO as stringio
import struct
import sys
import subprocess
import time
import traceback
import zipfile

log = logging.getLogger('pylvi')

bootstrap = """use File::Temp qw/tempfile/; sysread STDIN, $code, shift; ($fh, $fn) = tempfile; print $fh $code; exec "nice", "%s", "-u", $fn, @ARGV;"""

class Counter:
	def __init__(self):
		self.n = 0

	def next(self):
		self.n += 1
		return self.n

def moduledata(modulepaths):
	# TODO: search in sys.path also

	io = stringio.StringIO()
	zip = zipfile.ZipFile(io, "w")

	for root in modulepaths:
		root = root.split(".")[0]

		if not os.path.isdir(root):
			ok = False
			for suffix in ".py", ".pyc", ".pyo":
				path = root + suffix
				if os.path.exists(path):
					name = os.path.basename(path)
					zip.write(path, name)
					ok = True
			if ok:
				continue

		for dirpath, dirnames, filenames in os.walk(root):
			parent = os.path.dirname(root)
			dir = dirpath[len(parent):]
			if dir.startswith("/"):
				dir = dir[1:]
			for file in filenames:
				for suffix in ".py", ".pyc", ".pyo":
					if file.endswith(suffix):
						path = os.path.join(dirpath, file)
						name = os.path.join(dir, file)
						zip.write(path, name)
						break

	zip.close()
	return io.getvalue()

class Connection:
	def __init__(self, pylvi, name, p, poolsize = None):
		self.pylvi = pylvi

		self.name = name
		self.p = p

		self.infd = p.stdout.fileno()
		self.outfd = p.stdin.fileno()

		# non blocking io
		options = fcntl.fcntl(self.infd, fcntl.F_GETFL)
		fcntl.fcntl(self.infd, fcntl.F_SETFL, options | os.O_NONBLOCK)

		options = fcntl.fcntl(self.outfd, fcntl.F_GETFL)
		fcntl.fcntl(self.outfd, fcntl.F_SETFL, options | os.O_NONBLOCK)

		self.wantjobs = 0
		self.activejobs = 0
		self.poolsize = poolsize

		self.readbuffer = ""
		self.writebuffer = ""

		self.messagelen = None

	def wantread(self):
		return True

	def wantwrite(self):
		return self.writebuffer != ""

	def on_read(self):
		self.pylvi.readable(self)

	def on_write(self):
		self.push()

	def enqueue_message(self, msg):
		log.debug("(%s) enqueuing message(%s)", self.name, msg['type'])
		s = pickle.dumps(msg)
		h = struct.pack("I", len(s))
		self.writebuffer += h + s

		self.pylvi.wantwrite(self, True)

	def push(self):
		if self.writebuffer == "":
			return

		log.debug("(%s) pushing", self.name)

		try:
			written = os.write(self.outfd, self.writebuffer)
			#log.debug("written")

			self.writebuffer = self.writebuffer[written:]
		except OSError as e:
			if e.errno == errno.EAGAIN:
				pass
			else:
				raise e

		if self.writebuffer == "":
			self.pylvi.wantwrite(self, False)

	def write_message(self, msg):
		self.enqueue_message(msg)

	def read(self, size, timeout=None):
		if len(self.readbuffer) >= size:
			buf = self.readbuffer[0:size]
			self.readbuffer = self.readbuffer[size:]
			return buf

		try:
			tmp = os.read(self.infd, 65536)
			if tmp == "":
				raise IOError, "end-of-file"

			self.readbuffer = self.readbuffer + tmp

		except OSError as e:
			if e.errno == errno.EAGAIN:
				pass
			else:
				raise e

		if len(self.readbuffer) >= size:
			buf = self.readbuffer[0:size]
			self.readbuffer = self.readbuffer[size:]
			return buf

		return None

#	 def read_blocking(self, size):
#		while len(self.readbuffer) < size:
#			(r,w,x) = select.select([self.infd], [], [])
#
#			tmp = os.read(self.infd, 65536)
#			if tmp == "":
#				raise IOError, "end-of-file"
#
#			self.readbuffer = self.readbuffer + tmp
#
#		buf = self.readbuffer[0:size]
#		self.readbuffer = self.readbuffer[size:]
#		return buf

	def read_message(self):
		if self.messagelen is None:
			log.debug("(%s) reading message", self.name)

			header = self.read(4, timeout=1)
			if header == None:
				return None

			self.messagelen, = struct.unpack("I", header)

		msg = self.read(self.messagelen)
		if msg is None:
			return None

		self.messagelen = None
		return pickle.loads(msg)

	def __iter__(self):
		return self

	def next(self):
		msg = self.read_message()
		if msg is None:
			raise StopIteration

		return msg

class Result(object):
	"""Result wrapper, is able to raise exception in the callback"""

	def __init__(self, successful, result):
		self.successful = successful
		self.result = result

	def get(self):
		if self.successful:
			return self.result
		else:
			raise self.result

class GenericPylvi(object):
	def __init__(self, modules=[]):
		self.job_counter = Counter()
		self.jobs = collections.deque()
		self.jobs_iterators = collections.deque()
		self.jobs_active = {}

		self.wantjobs = 0

		self.connections = set()
		self.shuttingdown = False

		if modules == []:
			self.moduledata = None
		else:
			self.moduledata = moduledata(modules)
			log.info("moduledata size %d bytes", len(self.moduledata))

		# find slavecode
		self.slavecode = ""
		for prefix in sys.path:
			try:
				if prefix == "":
					prefix = "."

				if os.path.isdir(prefix):
					self.slavecode = file(os.path.join(prefix, "pylvi/slave.py")).read()
					break
			except IOError:
				pass

		if self.slavecode == "":
			raise Exception, "cannot find slave.py"

	def __enter__(self):
		return self

	def __exit__(self, type, value, tb):
		if type is not None:
			log.error("exception '%s' caught: %s", type.__name__, str(value))
			traceback.print_exception(type, value, tb, None, sys.stderr)
			return

		self.shuttingdown = True
		self.broadcast({'type': 'shutdown'})
		while len(self.connections) > 0:
			self.run()

	def _add_node(self, name, p, poolsize=None):
		connection = Connection(self, name, p, poolsize=poolsize)

		p.stdin.write(self.slavecode)
		p.stdin.flush()

		connection.write_message({'type': 'import', 'data': self.moduledata})

		log.info("adding node %s", connection.name)

		self.connections.add(connection)
		self.wantread(connection)

	def add_node(self, host, pythonpath=None, poolsize=None):
		if pythonpath is None:
			pythonpath = "python"

		p = subprocess.Popen(['ssh', host, 'perl', '-e', "'" + (bootstrap % pythonpath) +"'", str(len(self.slavecode))], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
		self._add_node(host, p, poolsize=poolsize)

	def add_local_node(self, pythonpath=None):
		if pythonpath is None:
			pythonpath = "python"

		p = subprocess.Popen(['perl', '-e', bootstrap % pythonpath, str(len(self.slavecode))], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
		self._add_node("local", p)

	def done(self):
		return len(self.jobs) == 0 and len(self.jobs_iterators) == 0 and self.jobs_active == {}

	def nextjob(self):
		try:
			return self.jobs.popleft()
		except IndexError:
			pass

		try:
			return self.jobs_iterators[0].next()
		except StopIteration:
			self.jobs_iterators.popleft()
		except IndexError:
			pass

		return None

	def nextconnection(self):
		for c in self.connections:
			if c.wantjobs > 0:
				return c

		return None

	def pushjob(self, connection, job):
		job_id = self.job_counter.next()
		log.info("pushing job %d", job_id)

		self.jobs_active[job_id] = job

		(func, args, kwargs, callback) = job
		connection.write_message({'type': 'job', 'jobid': job_id, 'job': (func, args, kwargs)})
		connection.wantjobs -= 1
		connection.activejobs += 1
		self.wantjobs -= 1

	def pushjobs(self):
		pushed = False

		if self.wantjobs > 0:
			log.debug("pushing jobs, want: %d, active: %d %s", self.wantjobs, len(self.jobs_active), str(self.jobs_active.keys()))

		while self.wantjobs > 0:

			job = self.nextjob()
			if job is None:
				break

			connection = self.nextconnection()
			if connection is None:
				log.critical("wantjobs > 0 but no greedy connection")
				self.jobs.append(job)
				break

			self.pushjob(connection, job)

			pushed = True

		return pushed

	def run(self, timeout=10):
		if len(self.connections) == 0:
			raise Exception, "no connections to communicate"

		self.pushjobs()

		fds = self.poll(timeout=timeout)

	def readable(self, connection):
		try:
			for msg in connection:
				self.process(connection, msg)

		except IOError as e:
			self.remove_connection(connection)

			if not self.shuttingdown:
				log.warn("io error %s", str(e))

	def remove_connection(self, connection):
			self.wantread(connection, False)
			log.debug("subprocess exited with code %d", connection.p.poll())
			self.connections.discard(connection)

	def process(self, conn, msg):
		t = msg['type']
		if t == 'pong':
			log.info("pong message received")

		elif t == 'newjob':
			if conn.poolsize is not None and conn.wantjobs + conn.activejobs == conn.poolsize:
				log.info("(%s) asks for more jobs, but limit of %d reached", conn.name, conn.poolsize)

			else:
				conn.wantjobs += 1
				self.wantjobs += 1
				log.debug("(%s) wants one new job, totally %d/%d", conn.name, conn.wantjobs, self.wantjobs)

		elif t == "jobdone":
			jobid = msg["jobid"]
			result = msg["result"]

			try:
				callback = self.jobs_active[jobid][3]

			except KeyError:
				log.warning("unknown job %d", jobid)

			try:
				callback(Result(True, result))
			except Exception as e:
				log.error("Exception '%s' caught during callback: %s", type(e).__name__, str(e))

			conn.wantjobs += 1
			conn.activejobs -= 1
			self.wantjobs += 1
			del self.jobs_active[jobid]

			log.debug("(%s) completed job, wants totally %d/%d", conn.name, conn.wantjobs, self.wantjobs)

			job = self.nextjob()
			if job is not None:
				self.pushjob(conn, job)

		elif t == "exception":
			e = msg['e']
			log.error("Remote exception '%s' caught during callback: %s", type(e).__name__, str(e))
			self.remove_connection(conn)

		else:
			log.warn("unknown message received: %s", t)

	def broadcast(self, msg):
		for c in self.connections:
			try:
				c.write_message(msg)
			except IOError as e:
				log.error("write error", e)

	def apply(self, func, args=None, kwargs={}, callback=None):
		"""Asynchronously apply function and call callback with the result"""

		self.jobs.append((func, args, kwargs, callback))

	def apply_many(self, job_iterator):
		self.jobs_iterators.append(job_iterator)

	def load_conf(self, filename):
		with file(filename) as conf:
			for line in conf:
				line = line.strip()
				if line == "" or line[0] == "#":
					continue

				toks = line.split()
				host = toks[0]
				pythonpath = None
				poolsize = None

				if len(toks) >= 2:
					pythonpath = toks[1]

				if len(toks) >= 3:
					poolsize = int(toks[2])

				self.add_node(toks[0], pythonpath=pythonpath, poolsize=poolsize)

class SelectPylvi(GenericPylvi):
	def __init__(self, *args, **kargs):
		super(SelectPylvi, self).__init__(*args, **kargs)
		self.__log = logging.getLogger('select-pylvi')
		self.fds = []

	def wantread(self, connection, state=True):
		pass

	def wantwrite(self, connection, state=False):
		pass

	def poll(self, timeout=1):
		readfds = []
		writefds = []
		fdmap = {}

		for c in self.connections:
			if c.wantread():
				readfds.append(c.infd)
				fdmap[c.infd] = c

			if c.wantwrite():
				writefds.append(c.outfd)
				fdmap[c.outfd] = c

		self.__log.debug("selecting %s %s", readfds, writefds)
		(rs,ws,x) = select.select(readfds, writefds, [], timeout)
		self.__log.debug("select returned %s %s", str(rs), str(ws))

		for r in rs:
			fdmap[r].on_read()

		for w in ws:
			fdmap[w].on_write()

Pylvi = SelectPylvi
