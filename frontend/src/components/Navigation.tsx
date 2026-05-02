import { NavLink } from "react-router-dom";

const links = [
  {
    to: "/attestazioni/generate",
    label: "Згенерувати Attestazione",
    hint: "Stage 7 UI integration",
  },
  {
    to: "/clients/bulk-import",
    label: "Додавання клієнтів в БД",
    hint: "Stage 8 UI integration",
  },
  {
    to: "/jobs",
    label: "Статус / Логи / Артефакти",
    hint: "Stage 9 data model",
  },
];

export default function Navigation() {
  return (
    <nav className="app-nav" aria-label="Основна навігація">
      <p className="eyebrow">Екрани</p>
      <ul className="app-nav__list">
        {links.map((link) => (
          <li key={link.to}>
            <NavLink
              to={link.to}
              className={({ isActive }) => (isActive ? "nav-link nav-link--active" : "nav-link")}
            >
              <span>{link.label}</span>
              <small>{link.hint}</small>
            </NavLink>
          </li>
        ))}
      </ul>
    </nav>
  );
}
