// A geometric mark built from the subject matter itself: a single grid cell
// (the unit this whole app is about) with a signal arc broadcasting from its
// center — grid + signal in one shape, rather than a generic abstract logo.
export default function Logo({ size = 28 }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 40 40"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
    >
      <rect x="2" y="2" width="36" height="36" rx="2" stroke="var(--ink)" strokeWidth="2" />
      <path
        d="M20 20 A7 7 0 0 1 27 27"
        stroke="var(--beacon)"
        strokeWidth="2.25"
        strokeLinecap="round"
        fill="none"
      />
      <path
        d="M20 20 A12 12 0 0 1 32 32"
        stroke="var(--beacon)"
        strokeWidth="2.25"
        strokeLinecap="round"
        fill="none"
        opacity="0.55"
      />
      <circle cx="20" cy="20" r="3" fill="var(--ink)" />
    </svg>
  );
}
