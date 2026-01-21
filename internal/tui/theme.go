package tui

import (
	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/lipgloss"
)

var (
	titleStyle    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("208"))
	selectedStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
	cursorStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("208"))
	checkStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("78"))
	dimStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	errorStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	successStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("78"))
	borderStyle   = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("208")).
			Padding(1, 2)
)

// themeAmber returns a custom huh theme with orange/purple/green colors
func themeAmber() *huh.Theme {
	t := huh.ThemeBase()

	var (
		normalFg = lipgloss.AdaptiveColor{Light: "235", Dark: "252"}
		orange   = lipgloss.Color("208")
		purple   = lipgloss.Color("141")
		green    = lipgloss.Color("78")
		gray     = lipgloss.Color("245")
		cream    = lipgloss.AdaptiveColor{Light: "#FFFDF5", Dark: "#FFFDF5"}
		red      = lipgloss.AdaptiveColor{Light: "#FF4672", Dark: "#ED567A"}
	)

	t.Focused.Base = t.Focused.Base.BorderForeground(lipgloss.Color("240"))
	t.Focused.Card = t.Focused.Base
	t.Focused.Title = t.Focused.Title.Foreground(purple).Bold(true)
	t.Focused.NoteTitle = t.Focused.NoteTitle.Foreground(purple).Bold(true).MarginBottom(1)
	t.Focused.Directory = t.Focused.Directory.Foreground(purple)
	t.Focused.Description = t.Focused.Description.Foreground(gray)
	t.Focused.ErrorIndicator = t.Focused.ErrorIndicator.Foreground(red)
	t.Focused.ErrorMessage = t.Focused.ErrorMessage.Foreground(red)
	t.Focused.SelectSelector = t.Focused.SelectSelector.Foreground(purple)
	t.Focused.NextIndicator = t.Focused.NextIndicator.Foreground(purple)
	t.Focused.PrevIndicator = t.Focused.PrevIndicator.Foreground(purple)
	t.Focused.Option = t.Focused.Option.Foreground(normalFg)
	t.Focused.MultiSelectSelector = t.Focused.MultiSelectSelector.Foreground(purple)
	t.Focused.SelectedOption = t.Focused.SelectedOption.Foreground(purple)
	t.Focused.SelectedPrefix = lipgloss.NewStyle().Foreground(green).SetString("✓ ")
	t.Focused.UnselectedPrefix = lipgloss.NewStyle().Foreground(gray).SetString("• ")
	t.Focused.UnselectedOption = t.Focused.UnselectedOption.Foreground(normalFg)
	t.Focused.FocusedButton = t.Focused.FocusedButton.Foreground(cream).Background(orange)
	t.Focused.Next = t.Focused.FocusedButton
	t.Focused.BlurredButton = t.Focused.BlurredButton.Foreground(normalFg).Background(lipgloss.AdaptiveColor{Light: "252", Dark: "238"})

	t.Focused.TextInput.Cursor = t.Focused.TextInput.Cursor.Foreground(purple)
	t.Focused.TextInput.Placeholder = t.Focused.TextInput.Placeholder.Foreground(gray)
	t.Focused.TextInput.Prompt = t.Focused.TextInput.Prompt.Foreground(purple)

	t.Blurred = t.Focused
	t.Blurred.Base = t.Focused.Base.BorderStyle(lipgloss.HiddenBorder())
	t.Blurred.Card = t.Blurred.Base
	t.Blurred.NextIndicator = lipgloss.NewStyle()
	t.Blurred.PrevIndicator = lipgloss.NewStyle()

	t.Group.Title = t.Focused.Title
	t.Group.Description = t.Focused.Description
	return t
}
