from distutils.core import setup

def read(path):
    return open(path).read()

setup(name='ezpool',
      version='0.1',
      py_modules= ['ezpool'
                   ],
      author = "Badi' Abdul-Wahid",
      author_email = 'abdulwahidc@gmail.com',
      license = 'BSD',
      long_description = read('README'),
      description = "Simple wrapper of python's multiprocessing Pool for parallel mapping"
      )
