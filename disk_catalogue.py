from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.schema import Table

Base = declarative_base()
engine = create_engine('postgresql+psycopg2:///disk-catalogue')
dealer = sessionmaker(bind=engine)
scoped_dealer = scoped_session(sessionmaker(bind=engine))


class Volume(Base):
    __table__ = Table(
        'volumes', Base.metadata,
        autoload=True,
        autoload_with=engine,
    )


class Inode(Base):
    __table__ = Table(
        'inodes', Base.metadata,
        autoload=True,
        autoload_with=engine,
    )


class Filename(Base):
    __table__ = Table(
        'filenames', Base.metadata,
        autoload=True,
        autoload_with=engine,
    )


class Content(Base):
    __table__ = Table(
        'content', Base.metadata,
        autoload=True,
        autoload_with=engine,
    )


class ContentInfo(Base):
    __table__ = Table(
        'content_info', Base.metadata,
        autoload=True,
        autoload_with=engine,
    )
