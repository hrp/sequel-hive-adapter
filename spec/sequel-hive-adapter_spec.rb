require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe "SequelHiveAdapter" do
  it "should load" do
    Sequel.connect("hive://localhost")
    #  fail "hey buddy, you should probably rename this file and start specing for real"
  end
end
