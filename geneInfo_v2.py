# encding: utf-8
from __future__ import print_function


import sys
try:
	from sqlalchemy import Column, create_engine
	from sqlalchemy.types import CHAR, Integer, String, BLOB
	from sqlalchemy.orm import sessionmaker
	from sqlalchemy.ext.declarative import declarative_base
except Exception as e:
	print(e)


# 创建对象的基类
Base = declarative_base()

# 定义KnowGene对象
class KnownGene(Base):
	# 表的名字
	__tablename__ = 'knownGene'
	# 表的结构
	name = Column(String(100), primary_key=True)
	chrom = Column(String(50), nullable=False)
	strand = Column(CHAR(10), nullable=False)
	txStart = Column(Integer, nullable=False)		# Integer不需要传参
	txEnd = Column(Integer, nullable=False)
	cdsStart = Column(Integer, nullable=False)
	cdsEnd = Column(Integer, nullable=False)
	exonCount = Column(Integer, nullable=False)
	exonStarts = Column(BLOB, nullable=False)
	exonEnds = Column(BLOB, nullable=False)
	proteinID = Column(String(100), nullable=False)
	alignID = Column(String(100), nullable=False)


# 定义KgXref对象
class KgXref(Base):
	__tablename__ = 'kgXref'
	# 表的结构
	kgID = Column(String(100), primary_key=True)
	mRNA = Column(String(100), nullable=False)
	spID = Column(String(100), nullable=False)
	spDisplayID = Column(String(100), nullable=False)
	geneSymbol = Column(String(100), nullable=False)
	refseq = Column(String(100), nullable=False)
	protAcc = Column(String(100), nullable=False)
	description = Column(BLOB, nullable=False)
	rfamAcc = Column(String(100), nullable=False)
	tRnaName = Column(String(100), nullable=False)


def getsession(user='genome', host='genome-mysql.soe.ucsc.edu', password='', port = 3306, database='hg38'):
	# 初始化UCSC数据库连接
	engine = create_engine('mysql+mysqldb://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(user, password, host, port, database))
	# 创建DBsession类型:
	DBsession = sessionmaker(bind=engine)
	# 创建session并返回session对象
	return DBsession()


# 根据mRNA来返回外显子物理位置
def query(q, session):
	kgxref= session.query(KgXref).filter(KgXref.mRNA==q.upper()).first()
	if kgxref:
		knowngene = session.query(KnownGene).filter(KnownGene.name==kgxref.kgID).first()
		return knowngene
	else:
		return None


# knowngene输出外显子信息
def outexoninfo(q, knowngene, f):
	if not knowngene:
		raise Exception("Please check {} is in UCSC database.".format(q))
	else:
		exonStarts = list(map(int, knowngene.exonStarts.strip(b',').split(b',')))
		exonEnds = list(map(int, knowngene.exonEnds.strip(b',').split(b',')))
		if knowngene.strand == '+':
			for i, start, stop in zip(range(1, knowngene.exonCount+1), exonStarts, exonEnds):
				f.write('{0}\t{1}\t{2}\t{3}\t{4}\n'.format(knowngene.chrom, start, stop, knowngene.strand, q))
		else:
			for i, start, stop in zip(range(1, knowngene.exonCount+1), reversed(exonStarts), reversed(exonEnds)):
				f.write('{0}\t{1}\t{2}\t{3}\t{4}\n'.format(knowngene.chrom, start, stop, knowngene.strand, q))


# knowngene输出CDS信息
def outCDSinfo(q, knowngene, f=sys.stdout):
	if not knowngene:
		raise Exception("Please check {} is in UCSC database.".format(q))
	else:
		f.write('{0}\t{1}\t{2}\t{3}\t{4}\n'.format(knowngene.chrom, knowngene.cdsStart, knowngene.cdsEnd, knowngene.strand, q))


if __name__ == '__main__':
	if len(sys.argv) == 2:
		fh = sys.stdout
	elif len(sys.argv) == 3:
		fh = sys.argv[2]
	else:
		raise Exception("Eror\nUsage: python {} [NM_xxx1] <stdout>\n".foramt(__file__))
	try:
		session = getsession()
		knowngene = query(sys.argv[1], session)
		sys.stdout.write("Start output {} CDS infomation.\n".format(sys.argv[1]))
		outCDSinfo(sys.argv[1], knowngene, fh)
		sys.stdout.write("Start output {} Exon information.\n".format(sys.argv[1]))
		outexoninfo(sys.argv[1], knowngene, fh)
	except Exception as e:
		pass
	finally:
		session.close()
