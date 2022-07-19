python
import sys
sys.path.insert(0, '/opt/compiler/gcc-10/share/gcc-10.1.0/python/')
sys.path.insert(0, '/home/zhangstar/.gdb/stlprettyprinter')
from libstdcxx.v6.printers import register_libstdcxx_printers
register_libstdcxx_printers (None)

# ClickHouse-pretty-printer-dir is the directory of this repo
# libcxx
#sys.path.insert(0, '{ClickHouse-pretty-printer-dir}')
#sys.path.insert(0, '/home/disk4/zhangstar/ClickHouse-pretty-printer/libcxx/v2')
#from libcxx_printers_new import register_libcxx_printer_loader
#register_libcxx_printer_loader()


# clickhouse pretty printer
#sys.path.insert(0, '{ClickHouse-pretty-printer-dir}/clickhouse')
sys.path.insert(0, '/home/disk4/zhangstar/ClickHouse-pretty-printer/clickhouse')
from printers import register_ch_printers
register_ch_printers()

# boost
#sys.path.insert(0, '{ClickHouse-pretty-printer-dir}/boost/share/boost-gdb-printers')
#sys.path.insert(0, '/home/disk4/zhangstar/ClickHouse-pretty-printer/boost/share/boost-gdb-printers')
#import boost.v1_57 as boost
#boost.register_pretty_printers(gdb)
end
