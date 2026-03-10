"""Repository for user operations."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.user import User


class UserRepository:
    """Repository for user CRUD operations."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def get_user_by_email(self, email: str) -> User | None:
        """Get a user by email."""
        stmt = select(User).where(User.email == email)
        return self.session.execute(stmt).scalar_one_or_none()

    def get_user_by_id(self, user_id: uuid.UUID) -> User | None:
        """Get a user by ID."""
        stmt = select(User).where(User.id == user_id)
        return self.session.execute(stmt).scalar_one_or_none()

    def create_user(self, email: str, hashed_password: str) -> User:
        """Create a new user."""
        user = User(email=email, hashed_password=hashed_password)
        self.session.add(user)
        self.session.commit()
        self.session.refresh(user)
        return user

    def update_user(self, user: User, **kwargs: Any) -> User:
        """Update a user."""
        for key, value in kwargs.items():
            if hasattr(user, key):
                setattr(user, key, value)
        self.session.commit()
        self.session.refresh(user)
        return user
