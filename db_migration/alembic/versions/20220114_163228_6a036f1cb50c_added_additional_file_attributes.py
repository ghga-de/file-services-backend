"""Added additional file attributes

Revision ID: 6a036f1cb50c
Revises: 826d7777c67c
Create Date: 2022-01-14 16:32:28.259435

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "6a036f1cb50c"
down_revision = "826d7777c67c"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("fileinfo", sa.Column("creation_date", sa.DateTime(), nullable=False))
    op.add_column("fileinfo", sa.Column("update_date", sa.DateTime(), nullable=False))
    op.add_column("fileinfo", sa.Column("format", sa.String(), nullable=False))
    op.add_column("fileinfo", sa.Column("size", sa.Integer(), nullable=False))
    op.drop_column("fileinfo", "registration_date")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "fileinfo",
        sa.Column(
            "registration_date",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.drop_column("fileinfo", "size")
    op.drop_column("fileinfo", "format")
    op.drop_column("fileinfo", "update_date")
    op.drop_column("fileinfo", "creation_date")
    # ### end Alembic commands ###
