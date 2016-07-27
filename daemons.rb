require 'rubygems'
require 'daemons'

filePfad = "/home/wetter/wetterDATA/"

options = {
  :log_output  => true,
  :monitor     => true,
  :log_dir     => filePfad + 'log/'
}

Daemons.run('dataCollector.rb', options)
