class Ini
  def self.read(filename, variable)
    File.open(filename).each{ |row|
    if row =~ Regexp.new("^" + variable + "=.*$")
      return row.scan(Regexp.new("^" + variable + "=(.*)$"))#[0]
    end
    }
  end
end


def tame(input)
        tamed = {}

        # split data on city names, throwing out surrounding brackets
        input = input.split(/\[([^\]]+)\]/)[1..-1]

        # sort the data into key/value pairs
        input.inject([]) {|tary, field|
                tary << field
                if(tary.length == 2)
                        # we have a key and value; put 'em to use
                        tamed[tary[0]] = tary[1].sub(/^\s+/,'').sub(/\s+$/,'')
                        # pass along a fresh temp-array
                        tary.clear
                end
                tary
                }

        tamed.dup.each { |tkey, tval|
                tvlist = tval.split(/[\r\n]+/)
                p tvlist
                tamed[tkey] = tvlist.inject({}) { |hash, val|
                        k, v = val.split(/=/)
                        hash[k]=v
                        hash
                        }
                }

        tamed
end

# Usefull staff. TODO: Make it robust.
class MyLib

  # Fixes slashes in windows paths.
  def self.fixPath(path)
    return path.gsub(/\\/, '/')
  end

  # Inspires by SQL ISNULL function.
  def self.isNull(value, default, exceptArray)
    return default if value == nil or value.empty? or exceptArray.include?(value)
    return value
  end

end
