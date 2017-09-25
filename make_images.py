#!/usr/bin/env python3

import os
import subprocess
import getpass
import requests
import tempfile

from urllib.parse import urljoin
from urllib.request import urlopen

DOCKER_ID = 'pathman'
ALPINE_BASE_URL = 'https://raw.githubusercontent.com/docker-library/postgres/master/10/alpine/'
ALPINE_ENTRYPOINT = 'docker-entrypoint.sh'

'''
How to create this patch:
	* put `import ipdb; ipdb.set_trace()` in make_alpine_image, just before `open(patch_name)..`
	* run the script
	* in temporary folder run `cp Dockerfile Dockerfile.1 && vim Dockerfile.1`
	* uncomment --enable-debug, add --enable-cassert, add `CFLAGS="-g3 -O0"` before ./configure
	* run `diff -Naur Dockerfile Dockerfile.1 > ./cassert.patch`
	* contents of cassert.patch put to variable below
	* change Dockerfile.1 to Dockerfile in text, change `\` symbols to `\\`
'''
ALPINE_PATCH = b'''
--- Dockerfile	2017-09-25 12:01:24.597813507 +0300
+++ Dockerfile	2017-09-25 12:09:06.104059704 +0300
@@ -79,15 +79,15 @@
 	&& wget -O config/config.sub 'https://git.savannah.gnu.org/cgit/config.git/plain/config.sub?id=7d3d27baf8107b630586c962c057e22149653deb' \\
 # configure options taken from:
 # https://anonscm.debian.org/cgit/pkg-postgresql/postgresql.git/tree/debian/rules?h=9.5
-	&& ./configure \\
+	&& CFLAGS="-g3 -O0" ./configure \\
 		--build="$gnuArch" \\
 # "/usr/src/postgresql/src/backend/access/common/tupconvert.c:105: undefined reference to `libintl_gettext'"
 #		--enable-nls \\
 		--enable-integer-datetimes \\
 		--enable-thread-safety \\
 		--enable-tap-tests \\
-# skip debugging info -- we want tiny size instead
-#		--enable-debug \\
+		--enable-debug \\
+		--enable-cassert \\
 		--disable-rpath \\
 		--with-uuid=e2fs \\
 		--with-gnu-ld \\
'''
CUSTOM_IMAGE_NAME = "%s/postgres_stable" % DOCKER_ID

def make_alpine_image(image_name):
	dockerfile = urlopen(urljoin(ALPINE_BASE_URL, 'Dockerfile')).read()
	entrypoint_sh = urlopen(urljoin(ALPINE_BASE_URL, ALPINE_ENTRYPOINT)).read()

	with tempfile.TemporaryDirectory() as tmpdir:
		print("Creating build in %s" % tmpdir)
		patch_name = os.path.join(tmpdir, "cassert.patch")

		with open(os.path.join(tmpdir, 'Dockerfile'), 'w') as f:
			f.write(dockerfile.decode())

		with open(os.path.join(tmpdir, ALPINE_ENTRYPOINT), 'w') as f:
			f.write(entrypoint_sh.decode())

		with open(patch_name, 'w') as f:
			f.write(ALPINE_PATCH.decode())

		with open(patch_name, 'r') as f:
			p = subprocess.Popen(["patch", "-p0"], cwd=tmpdir, stdin=subprocess.PIPE)
			p.communicate(str.encode(f.read()))
		print("patch applied")
		subprocess.check_output(["docker", "build", ".", '-t', image_name], cwd=tmpdir)
		print("build ok: ", image_name)
		subprocess.check_output(['docker', 'push', image_name],
				stderr=subprocess.STDOUT)
		print("upload ok:", image_name)

make_alpine_image(CUSTOM_IMAGE_NAME)

pg_containers = [
	('pg95', 'postgres:9.5-alpine'),
	('pg96', 'postgres:9.6-alpine'),
	('pg10', 'postgres:10-alpine'),
	('pg10_ca', CUSTOM_IMAGE_NAME),
]

image_types = {
	'clang_check_code': {
		'CHECK_CODE': 'clang',
	},
	'cppcheck': {
		'CHECK_CODE': 'cppcheck',
	},
	'pathman_tests': {
		'CHECK_CODE': 'false',
	}
}

user = input("Enter username for `docker login`: ")
password = getpass.getpass()
subprocess.check_output([
	'docker',
	'login',
	'-u', user,
	'-p', password])

travis_conf_line = '- DOCKER_IMAGE=%s'
travis_conf = []
print("")

if __name__ == '__main__':
	for pgname, container in pg_containers:
		for key, variables in image_types.items():
			image_name = '%s/%s_%s' % (DOCKER_ID, pgname, key)
			with open('Dockerfile', 'w') as out:
				with open('Dockerfile.tmpl', 'r') as f:
					for line in f:
						line = line.replace('${PG_IMAGE}', container)
						for key, value in variables.items():
							varname = '${%s}' % key
							line = line.replace(varname, value)

						out.write(line)

			args = [
				'docker',
				'build',
				'-t', image_name,
				'.'
			]
			subprocess.check_output(args, stderr=subprocess.STDOUT)
			print("build ok:", image_name)
			subprocess.check_output(['docker', 'push', image_name],
					stderr=subprocess.STDOUT)
			print("upload ok:", image_name)
			travis_conf.append(travis_conf_line % image_name)

print("\ntravis configuration")
print('\n'.join(travis_conf))
