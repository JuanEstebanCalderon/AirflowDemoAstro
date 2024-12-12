from sqlalchemy import (
    Column, Integer, String, Float, Date, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import uuid
from sqlalchemy.dialects.postgresql import UUID



Base = declarative_base()


# Modelo para la tabla tipo_transaccion
class TipoTransaccion(Base):
    __tablename__ = 'tipo_transaccion'

    id = Column(UUID, primary_key=True, default=uuid.uuid4)
    descripcion = Column(String(255), nullable=False)





# Modelo para la tabla Fecha
class Fecha(Base):
    __tablename__ = 'fecha'  # Cambiado a 'fecha'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha = Column(Date, nullable=False)
    dia = Column(Integer, nullable=False)
    mes = Column(Integer, nullable=False)
    anio = Column(String(255), nullable=False)





# Modelo para la tabla Transacciones
class Transacciones(Base):
    __tablename__ = 'transacciones'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha_id = Column(Integer, ForeignKey('fecha.id'), nullable=False)  # Actualizado a 'fecha'
    transaccion_tipo_id = Column(UUID, ForeignKey('tipo_transaccion.id'), nullable=False)
    simbolo = Column(String(255))
    precio = Column(Float)
    valor = Column(Float)

    # Relación con Fecha y TipoTransaccion
    fecha = relationship('Fecha', backref='transacciones')
    transaccion_tipo = relationship('TipoTransaccion', backref='transacciones')