require 'test_helper'

class LegantoSync1Test < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::LegantoSync1::VERSION
  end

  def test_migration
    m = LegantoSync1::Migration.new
    lists = ['http://lancaster.myreadinglists.org/lists/A56880F3-10B3-45EC-FD16-D29D0198AEE3']
    m.write('/home/lbaajh/tmp/aspire/lists/pub2015-17.csv', lists)
  end
end
