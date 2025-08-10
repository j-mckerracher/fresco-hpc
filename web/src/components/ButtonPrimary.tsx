import React from "react";

interface ButtonPrimaryProps {
  label: string;
  onClick: () => void;
  disabled?: boolean;
}

const ButtonPrimary: React.FC<ButtonPrimaryProps> = ({
  label,
  onClick = undefined,
  disabled = false,
}) => {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className="bg-purdue-boilermakerGold text-black font-semibold px-4 py-2 rounded-full text-2xl"
    >
      {label}
    </button>
  );
};

export default ButtonPrimary;
